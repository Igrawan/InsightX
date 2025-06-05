import express, { Application, Request, Response, NextFunction } from 'express';
import WebSocket from 'ws';
import { MongoClient, Db, Collection } from 'mongodb';
import dotenv from 'dotenv';
import { GoogleGenerativeAI } from '@google/generative-ai';

// Market models interfaces
export interface MarketData {
  symbol: string;
  price: number;
  change: number;
  volume: number;
  category: 'crypto' | 'forex' | 'stocks';
  description?: string;
  timestamp: number;
}

export interface TradingStrategy {
  name: string;
  type: 'aggressive' | 'moderate' | 'conservative' | 'balanced';
  description: string;
  confidence: number;
  timeframe: 'short' | 'medium' | 'long' | '5-15 minutes' | '2-6 hours' | '6-12 hours';
  advice: string;
}

export interface NewsItem {
  title: string;
  sentiment: 'bullish' | 'bearish' | 'neutral';
  impact: 'high' | 'medium' | 'low';
  timestamp: string;
  source?: string;
  summary?: string;
}

export interface MarketSentiment {
  bullish: number;
  bearish: number;
  neutral: number;
}

export interface MarketPsychology {
  fear: number;
  greed: number;
  momentum: number;
  volatility: number;
}

export interface MarketAnalysis {
  marketImpact: string;
  tradingOpportunities: string;
  keyRisks: string;
}

// Configuration
const GENAI_API_KEY = process.env.GENAI_API_KEY || 'AIzaSyD9rIIJJ1rdna65LtvJOGLFfA9lxMVzyag';
const BINANCE_WS_URL = 'wss://stream.binance.com:9443/ws';
const CACHE_TTL_MS = 60 * 60 * 1000; // 1 hour TTL for updates
const DATA_WINDOW_MS = 24 * 60 * 60 * 1000; // 24 hours
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://InsightX:Tamkhane12345@cluster0.5hity.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';
const DB_NAME = 'marketData';

const TIMEFRAMES = ['1h', '3h', '6h', '12h', '24h'] as const;
type Timeframe = typeof TIMEFRAMES[number];

const TIMEFRAME_MS: Record<Timeframe, number> = {
  '1h': 60 * 60 * 1000,
  '3h': 3 * 60 * 60 * 1000,
  '6h': 6 * 60 * 60 * 1000,
  '12h': 12 * 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000
};

// Cache interface
interface CacheEntry<T> {
  symbol: string;
  timeframe: Timeframe;
  data: T;
  timestamp: number;
}

// MongoDB client
class MongoDBClient {
  private client: MongoClient;
  private db: Db;
  private newsCollection: Collection<CacheEntry<NewsItem[]>>;
  private sentimentCollection: Collection<CacheEntry<MarketSentiment>>;
  private psychologyCollection: Collection<CacheEntry<MarketPsychology>>;
  private analysisCollection: Collection<CacheEntry<MarketAnalysis>>;
  private marketDataCollection: Collection<CacheEntry<MarketData>>;

  constructor() {
    this.client = new MongoClient(MONGODB_URI);
    this.db = this.client.db(DB_NAME);
    this.newsCollection = this.db.collection<CacheEntry<NewsItem[]>>('newsCache');
    this.sentimentCollection = this.db.collection<CacheEntry<MarketSentiment>>('sentimentCache');
    this.psychologyCollection = this.db.collection<CacheEntry<MarketPsychology>>('psychologyCache');
    this.analysisCollection = this.db.collection<CacheEntry<MarketAnalysis>>('analysisCache');
    this.marketDataCollection = this.db.collection<CacheEntry<MarketData>>('marketDataCache');
  }

  async connect() {
    try {
      await this.client.connect();
      console.log('Connected to MongoDB');
      await this.createIndexes();
    } catch (error) {
      console.error('Failed to connect to MongoDB:', error);
      throw error;
    }
  }

  private async createIndexes() {
    try {
      const collections = [
        this.newsCollection,
        this.sentimentCollection,
        this.psychologyCollection,
        this.analysisCollection,
        this.marketDataCollection
      ];

      for (const collection of collections) {
        try {
          await collection.dropIndex('symbol_1');
          console.log(`Dropped old symbol_1 index from ${collection.collectionName}`);
        } catch (error) {
          if (error instanceof Error && error.message.includes('index not found')) {
            console.log(`No symbol_1 index to drop in ${collection.collectionName}`);
          } else {
            console.error(`Error dropping symbol_1 index from ${collection.collectionName}:`, error);
          }
        }
      }

      await Promise.all([
        this.newsCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.sentimentCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.psychologyCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.analysisCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.marketDataCollection.createIndex({ symbol: 1, timeframe: 1 }, { unique: true }),
        this.newsCollection.createIndex({ timestamp: 1 }, { expireAfterSeconds: CACHE_TTL_MS / 1000 }),
        this.sentimentCollection.createIndex({ timestamp: 1 }, { expireAfterSeconds: CACHE_TTL_MS / 1000 }),
        this.psychologyCollection.createIndex({ timestamp: 1 }, { expireAfterSeconds: CACHE_TTL_MS / 1000 }),
        this.analysisCollection.createIndex({ timestamp: 1 }, { expireAfterSeconds: CACHE_TTL_MS / 1000 }),
        this.marketDataCollection.createIndex({ timestamp: 1 }, { expireAfterSeconds: CACHE_TTL_MS / 1000 })
      ]);

      console.log('Created new indexes successfully');
    } catch (error) {
      console.error('Error creating indexes:', error);
      throw error;
    }
  }

  async getCache<T>(collection: Collection<CacheEntry<T>>, symbol: string, timeframe: Timeframe): Promise<CacheEntry<T> | null> {
    return await collection.findOne({ symbol, timeframe });
  }

  async setCache<T>(collection: Collection<CacheEntry<T>>, symbol: string, timeframe: Timeframe, data: T, timestamp: number) {
    await collection.updateOne(
      { symbol, timeframe },
      { $set: { symbol, timeframe, data, timestamp } },
      { upsert: true }
    );
  }

  async deleteCache<T>(collection: Collection<CacheEntry<T>>, symbol: string, timeframe: Timeframe) {
    await collection.deleteOne({ symbol, timeframe });
  }

  async getNewsCache(symbol: string, timeframe: Timeframe): Promise<CacheEntry<NewsItem[]> | null> {
    return await this.getCache(this.newsCollection, symbol, timeframe);
  }

  async setNewsCache(symbol: string, timeframe: Timeframe, data: NewsItem[], timestamp: number) {
    await this.setCache(this.newsCollection, symbol, timeframe, data, timestamp);
  }

  async deleteNewsCache(symbol: string, timeframe: Timeframe) {
    await this.deleteCache(this.newsCollection, symbol, timeframe);
  }

  async getSentimentCache(symbol: string, timeframe: Timeframe): Promise<CacheEntry<MarketSentiment> | null> {
    return await this.getCache(this.sentimentCollection, symbol, timeframe);
  }

  async setSentimentCache(symbol: string, timeframe: Timeframe, data: MarketSentiment, timestamp: number) {
    await this.setCache(this.sentimentCollection, symbol, timeframe, data, timestamp);
  }

  async deleteSentimentCache(symbol: string, timeframe: Timeframe) {
    await this.deleteCache(this.sentimentCollection, symbol, timeframe);
  }

  async getPsychologyCache(symbol: string, timeframe: Timeframe): Promise<CacheEntry<MarketPsychology> | null> {
    return await this.getCache(this.psychologyCollection, symbol, timeframe);
  }

  async setPsychologyCache(symbol: string, timeframe: Timeframe, data: MarketPsychology, timestamp: number) {
    await this.setCache(this.psychologyCollection, symbol, timeframe, data, timestamp);
  }

  async deletePsychologyCache(symbol: string, timeframe: Timeframe) {
    await this.deleteCache(this.psychologyCollection, symbol, timeframe);
  }

  async getAnalysisCache(symbol: string, timeframe: Timeframe): Promise<CacheEntry<MarketAnalysis> | null> {
    return await this.getCache(this.analysisCollection, symbol, timeframe);
  }

  async setAnalysisCache(symbol: string, timeframe: Timeframe, data: MarketAnalysis, timestamp: number) {
    await this.setCache(this.analysisCollection, symbol, timeframe, data, timestamp);
  }

  async deleteAnalysisCache(symbol: string, timeframe: Timeframe) {
    await this.deleteCache(this.analysisCollection, symbol, timeframe);
  }

  async getMarketDataCache(symbol: string, timeframe: Timeframe): Promise<CacheEntry<MarketData> | null> {
    return await this.getCache(this.marketDataCollection, symbol, timeframe);
  }

  async setMarketDataCache(symbol: string, timeframe: Timeframe, data: MarketData, timestamp: number) {
    await this.setCache(this.marketDataCollection, symbol, timeframe, data, timestamp);
  }

  async deleteMarketDataCache(symbol: string, timeframe: Timeframe) {
    await this.deleteCache(this.marketDataCollection, symbol, timeframe);
  }

  async getAllCachedSymbols(): Promise<string[]> {
    return await this.newsCollection.distinct('symbol');
  }

  async getHighImpactNews(timeframe: Timeframe): Promise<{ symbol: string, news: NewsItem }[]> {
    const symbols = await this.getAllCachedSymbols();
    const highImpactNews: { symbol: string, news: NewsItem }[] = [];
    
    for (const symbol of symbols) {
      const cacheEntry = await this.getNewsCache(symbol, timeframe);
      if (cacheEntry?.data) {
        const highImpactItems = cacheEntry.data
          .filter(item => item.impact === 'high')
          .map(item => ({ symbol, news: item }));
        highImpactNews.push(...highImpactItems);
      }
    }

    return highImpactNews.sort((a, b) => 
      new Date(b.news.timestamp).getTime() - new Date(a.news.timestamp).getTime()
    );
  }

  async cleanupOldData() {
    try {
      const collections = [
        this.newsCollection,
        this.sentimentCollection,
        this.psychologyCollection,
        this.analysisCollection,
        this.marketDataCollection
      ];

      await Promise.all(
        collections.map(collection => 
          collection.deleteMany({ timeframe: { $exists: false } })
            .then(() => console.log(`Cleaned up old data from ${collection.collectionName}`))
        )
      );
    } catch (error) {
      console.error('Error cleaning up old data:', error);
    }
  }
}

// Google GenAI client
class GoogleGenAIClient {
  private genAI: GoogleGenerativeAI;
  private model: any;

  constructor() {
    this.genAI = new GoogleGenerativeAI(GENAI_API_KEY);
    this.model = this.genAI.getGenerativeModel({ model: 'gemini-1.5-flash' });
  }

  async fetchRealTimeNews(symbol: string, timeframe: Timeframe): Promise<NewsItem[]> {
    try {
      const normalizedSymbol = symbol.split('/')[0].toUpperCase();
      const endTime = new Date();
      const startTime = new Date(endTime.getTime() - TIMEFRAME_MS[timeframe]);

      let lastError: Error | null = null;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
          const prompt = `Provide news analysis for ${normalizedSymbol} cryptocurrency from ${startTime.toISOString()} to ${endTime.toISOString()}. Include sentiment (bullish, bearish, neutral), impact level (high, medium, low), and brief summaries. Return in JSON format with fields: title, sentiment, impact, timestamp (ISO format), source, summary. Limit to 10 items per ${timeframe}, prioritizing high-impact news, sorted by recency (newest first) and then by impact (high to low).`;

          const result = await this.model.generateContent(prompt);
          const response = await result.response;
          let jsonString = response.text().trim();

          if (jsonString.startsWith('```json') && jsonString.endsWith('```')) {
            jsonString = jsonString.slice(7, -3).trim();
          } else if (jsonString.startsWith('```') && jsonString.endsWith('```')) {
            jsonString = jsonString.slice(3, -3).trim();
          }

          const newsData: NewsItem[] = JSON.parse(jsonString);
          const impactOrder = { high: 3, medium: 2, low: 1 };

          return newsData
            .filter(item => {
              const itemTime = new Date(item.timestamp);
              return item.title && 
                ['bullish', 'bearish', 'neutral'].includes(item.sentiment) &&
                ['high', 'medium', 'low'].includes(item.impact) &&
                itemTime >= startTime && itemTime <= endTime;
            })
            .sort((a, b) => {
              const timeDiff = new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime();
              return timeDiff !== 0 ? timeDiff : impactOrder[b.impact] - impactOrder[a.impact];
            })
            .slice(0, 10);
        } catch (error) {
          lastError = error as Error;
          if (attempt < MAX_RETRIES) {
            await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
          }
        }
      }
      throw lastError || new Error('Failed to fetch news after retries');
    } catch (error) {
      console.error(`Error fetching news for ${symbol} (${timeframe}):`, error);
      return [];
    }
  }
}

// Binance WebSocket client
class BinanceWSClient {
  private ws: WebSocket;
  private marketDataHistory: Map<string, MarketData[]> = new Map();
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 5;

  constructor() {
    this.ws = new WebSocket(BINANCE_WS_URL);
    this.setupWebSocket();
  }

  private setupWebSocket() {
    this.ws.on('open', () => {
      this.reconnectAttempts = 0;
      const symbols = ['btcusdt', 'ethusdt', 'xrpusdt', 'bnbusdt', 'solusdt'];
      symbols.forEach(symbol => {
        this.ws.send(JSON.stringify({
          method: 'SUBSCRIBE',
          params: [`${symbol}@ticker`],
          id: Date.now()
        }));
      });
    });

    this.ws.on('message', (data: Buffer) => {
      const message = JSON.parse(data.toString());
      if (message.e === '24hrTicker') {
        this.updateMarketData(message);
      }
    });

    this.ws.on('error', (error) => {
      console.error('WebSocket error:', error);
    });

    this.ws.on('close', () => {
      if (this.reconnectAttempts < this.maxReconnectAttempts) {
        this.reconnectAttempts++;
        setTimeout(() => {
          this.ws = new WebSocket(BINANCE_WS_URL);
          this.setupWebSocket();
        }, RETRY_DELAY_MS * this.reconnectAttempts);
      } else {
        console.error('Max reconnect attempts reached. WebSocket connection failed.');
      }
    });
  }

  private updateMarketData(ticker: any) {
    const symbol = `${ticker.s.split('USDT')[0]}/USD`;
    const marketData: MarketData = {
      symbol,
      price: parseFloat(ticker.c),
      change: parseFloat(ticker.P),
      volume: parseFloat(ticker.v),
      category: 'crypto',
      timestamp: Date.now()
    };

    if (!this.marketDataHistory.has(symbol)) {
      this.marketDataHistory.set(symbol, []);
    }
    const history = this.marketDataHistory.get(symbol)!;
    history.push(marketData);

    const cutoff = Date.now() - DATA_WINDOW_MS;
    this.marketDataHistory.set(symbol, history.filter(data => data.timestamp >= cutoff));
  }

  getMarketData(symbol: string, timeframe: Timeframe): MarketData | undefined {
    const history = this.marketDataHistory.get(symbol);
    if (!history || history.length === 0) return undefined;

    const cutoff = Date.now() - TIMEFRAME_MS[timeframe];
    const relevantData = history.filter(data => data.timestamp >= cutoff);

    if (relevantData.length === 0) return undefined;

    const avgPrice = relevantData.reduce((sum, data) => sum + data.price, 0) / relevantData.length;
    const totalVolume = relevantData.reduce((sum, data) => sum + data.volume, 0);
    const firstPrice = relevantData[0].price;
    const lastPrice = relevantData[relevantData.length - 1].price;
    const change = ((lastPrice - firstPrice) / firstPrice) * 100;

    return {
      symbol,
      price: avgPrice,
      change,
      volume: totalVolume,
      category: 'crypto',
      timestamp: Date.now()
    };
  }
}

export class MarketDataService {
  private genAIClient: GoogleGenAIClient;
  private binanceClient: BinanceWSClient;
  private mongoClient: MongoDBClient;

  constructor() {
    this.genAIClient = new GoogleGenAIClient();
    this.binanceClient = new BinanceWSClient();
    this.mongoClient = new MongoDBClient();
    this.initialize();
  }

  private async initialize() {
    await this.mongoClient.connect();
    await this.mongoClient.cleanupOldData();
    this.scheduleHourlyUpdates();
  }

  private scheduleHourlyUpdates() {
    const now = new Date();
    const msUntilNextHour = (60 - now.getMinutes()) * 60 * 1000 - now.getSeconds() * 1000 - now.getMilliseconds();

    setTimeout(() => {
      this.updateCaches();
      setInterval(() => this.updateCaches(), CACHE_TTL_MS);
    }, msUntilNextHour);
  }

  private async updateCaches() {
    console.log('Updating caches for hourly refresh');
    const now = new Date();
    const symbols = await this.mongoClient.getAllCachedSymbols();

    for (const symbol of symbols) {
      for (const timeframe of TIMEFRAMES) {
        try {
          const startTime = new Date(now.getTime() - TIMEFRAME_MS[timeframe]);
          const newNews = await this.genAIClient.fetchRealTimeNews(symbol, timeframe);
          
          const cacheEntry = await this.mongoClient.getNewsCache(symbol, timeframe);
          let allNews = cacheEntry?.data || [];
          allNews = [...allNews, ...newNews].filter(item => {
            const itemTime = new Date(item.timestamp);
            return itemTime >= startTime && itemTime <= now;
          });

          await Promise.all([
            this.mongoClient.setNewsCache(symbol, timeframe, allNews, Date.now()),
            this.mongoClient.deleteSentimentCache(symbol, timeframe),
            this.mongoClient.deletePsychologyCache(symbol, timeframe),
            this.mongoClient.deleteAnalysisCache(symbol, timeframe),
            this.mongoClient.deleteMarketDataCache(symbol, timeframe)
          ]);
        } catch (error) {
          console.error(`Error updating cache for ${symbol} (${timeframe}):`, error);
        }
      }
    }
  }

  private isCacheValid<T>(cacheEntry: CacheEntry<T> | null): boolean {
    if (!cacheEntry) return false;
    const age = Date.now() - cacheEntry.timestamp;
    return age <= CACHE_TTL_MS;
  }

  async getAllMarkets(timeframe: Timeframe): Promise<MarketData[]> {
    const symbols = Array.from(this.binanceClient['marketDataHistory'].keys());
    const markets: MarketData[] = [];
    for (const symbol of symbols) {
      const marketData = await this.getMarketBySymbol(symbol, timeframe);
      if (marketData) markets.push(marketData);
    }
    return markets;
  }

  async getMarketBySymbol(symbol: string, timeframe: Timeframe): Promise<MarketData | undefined> {
    const cacheEntry = await this.mongoClient.getMarketDataCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      return cacheEntry!.data;
    }

    const marketData = this.binanceClient.getMarketData(symbol, timeframe);
    if (marketData) {
      await this.mongoClient.setMarketDataCache(symbol, timeframe, marketData, Date.now());
    }
    return marketData;
  }

  async getHighImpactNews(timeframe: Timeframe): Promise<{ symbol: string, news: NewsItem }[]> {
    return await this.mongoClient.getHighImpactNews(timeframe);
  }

  async getTradingStrategies(symbol: string, timeframe: Timeframe): Promise<TradingStrategy[]> {
    const [market, news, sentiment] = await Promise.all([
      this.getMarketBySymbol(symbol, timeframe),
      this.getMarketNews(symbol, timeframe),
      this.getMarketSentiment(symbol, timeframe)
    ]);

    if (!market) return [];

    const priceChange = market.change;
    const volume = market.volume;
    const isBullish = sentiment.bullish > sentiment.bearish;
    const newsImpact = news.length > 0 ? news[0].impact : 'medium';

    const baseConfidence = newsImpact === 'high' ? 90 : newsImpact === 'medium' ? 80 : 70;
    const volumeIndicator = volume > 1000000 ? 'high' : volume > 500000 ? 'moderate' : 'low';
    const volatilityFactor = Math.abs(priceChange) > 5 ? 'high' : Math.abs(priceChange) > 2 ? 'moderate' : 'low';

    const timeframeStrategies: Record<Timeframe, { name: string, type: string, timeframe: string }> = {
      '1h': { name: 'Hourly Surge', type: 'aggressive', timeframe: '5-15 minutes' },
      '3h': { name: 'Quarter-Day Momentum', type: 'balanced', timeframe: '2-6 hours' },
      '6h': { name: 'Half-Day Trend', type: 'balanced', timeframe: '2-6 hours' },
      '12h': { name: 'Intraday Trend', type: 'conservative', timeframe: '6-12 hours' },
      '24h': { name: 'Daily Trend', type: 'conservative', timeframe: '6-12 hours' }
    };

    const strategy = timeframeStrategies[timeframe];

    return [
      {
        name: strategy.name,
        type: strategy.type as 'aggressive' | 'balanced' | 'conservative',
        description: isBullish
          ? `The market shows ${timeframe} bullish pressure with ${sentiment.bullish.toFixed(1)}% bullish sentiment and a price change of ${priceChange.toFixed(2)}%. High trading volume (${volumeIndicator}) and recent news (impact: ${newsImpact}) suggest opportunities for ${strategy.type} trading. Volatility is ${volatilityFactor}.`
          : `The market exhibits ${timeframe} bearish pressure with ${sentiment.bearish.toFixed(1)}% bearish sentiment and a price change of ${priceChange.toFixed(2)}%. Recent news (impact: ${newsImpact}) and ${volumeIndicator} volume create opportunities for ${strategy.type} trading. Volatility is ${volatilityFactor}.`,
        confidence: Math.min(95, baseConfidence + (volumeIndicator === 'high' ? 5 : 0)),
        timeframe: strategy.timeframe as '5-15 minutes' | '2-6 hours' | '6-12 hours',
        advice: isBullish
          ? `Enter long positions on dips within ${timeframe}, targeting quick profits. Use tight stop-losses due to ${volatilityFactor} volatility. Monitor high-impact news.`
          : `Enter short positions or wait for bounces within ${timeframe}. Set stop-losses to manage ${volatilityFactor} volatility. Watch for news-driven reversals.`
      }
    ];
  }

  async getMarketNews(symbol: string, timeframe: Timeframe): Promise<NewsItem[]> {
    const cacheEntry = await this.mongoClient.getNewsCache(symbol, timeframe);
    const now = new Date();
    const cutoffTime = new Date(now.getTime() - TIMEFRAME_MS[timeframe]);

    if (this.isCacheValid(cacheEntry)) {
      return cacheEntry!.data.filter(item => new Date(item.timestamp) >= cutoffTime);
    }

    const news = await this.genAIClient.fetchRealTimeNews(symbol, timeframe);
    await this.mongoClient.setNewsCache(symbol, timeframe, news, Date.now());
    return news;
  }

  async getMarketSentiment(symbol: string, timeframe: Timeframe): Promise<MarketSentiment> {
    const cacheEntry = await this.mongoClient.getSentimentCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      return cacheEntry!.data;
    }

    const [news, market] = await Promise.all([
      this.getMarketNews(symbol, timeframe),
      this.getMarketBySymbol(symbol, timeframe)
    ]);

    const sentimentCount = news.reduce((acc, item) => {
      acc[item.sentiment]++;
      return acc;
    }, { bullish: 0, bearish: 0, neutral: 0 });

    const totalNews = news.length || 1;
    let baseBullish = sentimentCount.bullish / totalNews;
    let baseBearish = sentimentCount.bearish / totalNews;
    let baseNeutral = sentimentCount.neutral / totalNews;

    if (market) {
      const priceChange = market.change;
      const volume = market.volume;

      const priceImpact = Math.abs(priceChange) / 10;
      if (priceChange > 0) {
        baseBullish += priceImpact * 0.3;
        baseNeutral -= priceImpact * 0.15;
        baseBearish -= priceImpact * 0.15;
      } else if (priceChange < 0) {
        baseBearish += priceImpact * 0.3;
        baseNeutral -= priceImpact * 0.15;
        baseBullish -= priceImpact * 0.15;
      }

      const volumeFactor = volume > 1000000 ? 0.2 : volume > 500000 ? 0.1 : 0;
      if (baseBullish > baseBearish) {
        baseBullish += volumeFactor;
        baseBearish -= volumeFactor / 2;
        baseNeutral -= volumeFactor / 2;
      } else if (baseBearish > baseBullish) {
        baseBearish += volumeFactor;
        baseBullish -= volumeFactor / 2;
        baseNeutral -= volumeFactor / 2;
      }

      // Normalize to ensure sum equals 100%
      const total = baseBullish + baseBearish + baseNeutral;
      if (total > 0) {
        baseBullish = (baseBullish / total) * 100;
        baseBearish = (baseBearish / total) * 100;
        baseNeutral = (baseNeutral / total) * 100;
      } else {
        // Fallback to equal distribution if total is 0
        baseBullish = 33.33;
        baseBearish = 33.33;
        baseNeutral = 33.34;
      }

      // Ensure sum is exactly 100% by adjusting neutral if necessary
      const sum = baseBullish + baseBearish + baseNeutral;
      if (sum !== 100) {
        baseNeutral += 100 - sum;
      }
    } else {
      // If no market data, use news-based sentiment with normalization
      baseBullish = (baseBullish / totalNews) * 100;
      baseBearish = (baseBearish / totalNews) * 100;
      baseNeutral = (baseNeutral / totalNews) * 100;

      const sum = baseBullish + baseBearish + baseNeutral;
      if (sum > 0) {
        baseBullish = (baseBullish / sum) * 100;
        baseBearish = (baseBearish / sum) * 100;
        baseNeutral = (baseNeutral / sum) * 100;
      } else {
        baseBullish = 33.33;
        baseBearish = 33.33;
        baseNeutral = 33.34;
      }

      const sumAfter = baseBullish + baseBearish + baseNeutral;
      if (sumAfter !== 100) {
        baseNeutral += 100 - sumAfter;
      }
    }

    const sentiment = {
      bullish: Math.max(0, Math.min(100, Number(baseBullish.toFixed(2)))),
      bearish: Math.max(0, Math.min(100, Number(baseBearish.toFixed(2)))),
      neutral: Math.max(0, Math.min(100, Number(baseNeutral.toFixed(2))))
    };

    await this.mongoClient.setSentimentCache(symbol, timeframe, sentiment, Date.now());
    return sentiment;
  }

  async getMarketPsychology(symbol: string, timeframe: Timeframe): Promise<MarketPsychology> {
    const cacheEntry = await this.mongoClient.getPsychologyCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      return cacheEntry!.data;
    }

    const [market, news, sentiment] = await Promise.all([
      this.getMarketBySymbol(symbol, timeframe),
      this.getMarketNews(symbol, timeframe),
      this.getMarketSentiment(symbol, timeframe)
    ]);

    if (!market) {
      return { fear: 0, greed: 0, momentum: 0, volatility: 0 };
    }

    const priceChange = market.change;
    const volume = market.volume;

    const newsImpactScore = news.reduce((acc, item) => {
      const impactValue = { high: 3, medium: 2, low: 1 }[item.impact];
      const sentimentMultiplier = item.sentiment === 'bullish' ? 1 : item.sentiment === 'bearish' ? -1 : 0;
      return acc + (impactValue * sentimentMultiplier);
    }, 0);

    const avgImpact = news.length > 0 
      ? news.reduce((acc, item) => acc + ({ high: 3, medium: 2, low: 1 }[item.impact]), 0) / news.length 
      : 1;

    const psychology = {
      fear: Math.min(100, Math.max(
        (priceChange < 0 ? Math.abs(priceChange) * 5 : 0) + 
        (sentiment.bearish * 0.5) + 
        (newsImpactScore < 0 ? Math.abs(newsImpactScore) * 2 : 0),
        0
      )),
      greed: Math.min(100, Math.max(
        (priceChange > 0 ? priceChange * 5 : 0) + 
        (sentiment.bullish * 0.5) + 
        (newsImpactScore > 0 ? newsImpactScore * 2 : 0),
        0
      )),
      momentum: Math.min(100, 
        (volume / 1000000) * 10 + 
        Math.max(sentiment.bullish, sentiment.bearish) * 0.2 + 
        avgImpact * 5
      ),
      volatility: Math.min(100, 
        Math.abs(priceChange) * 5 + 
        (avgImpact - 1) * 20 + 
        (news.length > 5 ? 10 : 0)
      )
    };

    await this.mongoClient.setPsychologyCache(symbol, timeframe, psychology, Date.now());
    return psychology;
  }

  async getMarketAnalysis(symbol: string, timeframe: Timeframe): Promise<MarketAnalysis> {
    const cacheEntry = await this.mongoClient.getAnalysisCache(symbol, timeframe);
    if (this.isCacheValid(cacheEntry)) {
      return cacheEntry!.data;
    }

    const [news, sentiment, market, psychology] = await Promise.all([
      this.getMarketNews(symbol, timeframe),
      this.getMarketSentiment(symbol, timeframe),
      this.getMarketBySymbol(symbol, timeframe),
      this.getMarketPsychology(symbol, timeframe)
    ]);

    const impactScore = news.reduce((acc, item) => {
      const impactValue = { high: 3, medium: 2, low: 1 }[item.impact];
      const sentimentMultiplier = item.sentiment === 'bullish' ? 1 : item.sentiment === 'bearish' ? -1 : 0;
      return acc + (impactValue * sentimentMultiplier);
    }, 0);

    let tradingOpportunities = "";
    if (!market) {
      tradingOpportunities = `Hey there! I'm sorry, but I don't have enough market data for ${symbol} over the ${timeframe} timeframe to give you solid trading advice right now. Try checking back later or switching to a different timeframe!`;
    } else {
      const priceTrend = market.change > 0 ? "climbing" : market.change < 0 ? "dipping" : "holding steady";
      const volumeIndicator = market.volume > 1000000 ? "pretty active" : market.volume > 500000 ? "decent" : "a bit quiet";
      const bias = impactScore > 0 ? "bullish" : impactScore < 0 ? "bearish" : "neutral";

      tradingOpportunities = `Alright, let's break it down for ${symbol} over the last ${timeframe}! The price is ${priceTrend} with a ${volumeIndicator} trading volume and a ${bias} vibe from the news (impact score: ${impactScore}). Sentiment is looking ${sentiment.bullish.toFixed(1)}% bullish, ${sentiment.bearish.toFixed(1)}% bearish, and ${sentiment.neutral.toFixed(1)}% neutral. On the psychology side, we're seeing ${psychology.fear.toFixed(1)}% fear, ${psychology.greed.toFixed(1)}% greed, and ${psychology.volatility.toFixed(1)}% volatility. `;

      if (impactScore > 2 && psychology.greed > 50 && sentiment.bullish > 50) {
        tradingOpportunities += `This looks like a great moment to jump in! Consider going long on any pullbacks within ${timeframe} to ride the momentum. Just keep your stop-losses tight to manage that ${psychology.volatility.toFixed(1)}% volatility.`;
      } else if (impactScore < -2 && psychology.fear > 50 && sentiment.bearish > 50) {
        tradingOpportunities += `Things are looking a bit shaky, so you might want to consider short positions or hold off for a bounce within ${timeframe}. Set those stop-losses to navigate the ${psychology.volatility.toFixed(1)}% volatility.`;
      } else {
        tradingOpportunities += `The market's in a bit of a holding pattern, so I'd play it safe. Look for range-bound trades or wait for a breakout within ${timeframe}. Keep stop-losses in place to handle the ${psychology.volatility.toFixed(1)}% volatility.`;
      }
    }

    const analysis = {
      marketImpact: `${news.length} news items analyzed in the last ${timeframe}. Impact Score: ${impactScore}`,
      tradingOpportunities,
      keyRisks: `Keep an eye out for sudden shifts driven by high-impact news or big volume spikes within ${timeframe}.`
    };

    await this.mongoClient.setAnalysisCache(symbol, timeframe, analysis, Date.now());
    return analysis;
  }
}

export class MarketController {
  private static service = new MarketDataService();

  private static getTimeframe(req: Request): Timeframe {
    const timeframe = req.query.timeframe as string;
    return TIMEFRAMES.includes(timeframe as Timeframe) ? timeframe as Timeframe : '24h';
  }

  static async getAllMarkets(req: Request, res: Response) {
    try {
      const timeframe = MarketController.getTimeframe(req);
      const markets = await MarketController.service.getAllMarkets(timeframe);
      res.json(markets);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch markets' });
    }
  }

  static async getMarketBySymbol(req: Request<{ symbol: string }>, res: Response) {
    try {
      const symbol = decodeURIComponent(req.params.symbol);
      const timeframe = MarketController.getTimeframe(req);
      const market = await MarketController.service.getMarketBySymbol(symbol, timeframe);
      market ? res.json(market) : res.status(404).json({ error: 'Market not found' });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch market data' });
    }
  }

  static async getTradingStrategies(req: Request<{ symbol: string }>, res: Response) {
    try {
      const symbol = decodeURIComponent(req.params.symbol);
      const timeframe = MarketController.getTimeframe(req);
      const strategies = await MarketController.service.getTradingStrategies(symbol, timeframe);
      res.json(strategies);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch trading strategies' });
    }
  }

  static async getMarketNews(req: Request<{ symbol: string }>, res: Response) {
    try {
      const symbol = decodeURIComponent(req.params.symbol);
      const timeframe = MarketController.getTimeframe(req);
      const news = await MarketController.service.getMarketNews(symbol, timeframe);
      res.json(news);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch market news' });
    }
  }

  static async getMarketSentiment(req: Request<{ symbol: string }>, res: Response) {
    try {
      const symbol = decodeURIComponent(req.params.symbol);
      const timeframe = MarketController.getTimeframe(req);
      const sentiment = await MarketController.service.getMarketSentiment(symbol, timeframe);
      res.json(sentiment);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch market sentiment' });
    }
  }

  static async getMarketPsychology(req: Request<{ symbol: string }>, res: Response) {
    try {
      const symbol = decodeURIComponent(req.params.symbol);
      const timeframe = MarketController.getTimeframe(req);
      const psychology = await MarketController.service.getMarketPsychology(symbol, timeframe);
      res.json(psychology);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch market psychology' });
    }
  }

  static async getMarketAnalysis(req: Request<{ symbol: string }>, res: Response) {
    try {
      const symbol = decodeURIComponent(req.params.symbol);
      const timeframe = MarketController.getTimeframe(req);
      const analysis = await MarketController.service.getMarketAnalysis(symbol, timeframe);
      res.json(analysis);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch market analysis' });
    }
  }

  static async getHighImpactNews(req: Request, res: Response) {
    try {
      const timeframe = MarketController.getTimeframe(req);
      const highImpactNews = await MarketController.service.getHighImpactNews(timeframe);
      res.json(highImpactNews);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch high-impact news' });
    }
  }
}

// Load environment variables
dotenv.config();

const app: Application = express();
const PORT = process.env.PORT || 3005;

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Routes
app.get('/markets', MarketController.getAllMarkets);
app.get(
  '/markets/:symbol([A-Z]{2,5}/[A-Z]{3}|[A-Z]{2,5})',
  MarketController.getMarketBySymbol
);
app.get(
  '/markets/:symbol([A-Z]{2,5}/[A-Z]{3}|[A-Z]{2,5})/strategies',
  MarketController.getTradingStrategies
);
app.get(
  '/markets/:symbol([A-Z]{2,5}/[A-Z]{3}|[A-Z]{2,5})/news',
  MarketController.getMarketNews
);
app.get(
  '/markets/:symbol([A-Z]{2,5}/[A-Z]{3}|[A-Z]{2,5})/sentiment',
  MarketController.getMarketSentiment
);
app.get(
  '/markets/:symbol([A-Z]{2,5}/[A-Z]{3}|[A-Z]{2,5})/psychology',
  MarketController.getMarketPsychology
);
app.get(
  '/markets/:symbol([A-Z]{2,5}/[A-Z]{3}|[A-Z]{2,5})/analysis',
  MarketController.getMarketAnalysis
);
app.get('/notifications/high-impact-news', MarketController.getHighImpactNews);

// Error handling middleware
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  console.error(err.stack);
  res.status(500).json({ error: 'Something went wrong!' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

// Handle unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});