// index.js - SIÊU AI TÀI XỈU - 30+ THUẬT TOÁN PRO HOÀN CHỈNH
// Nguồn dữ liệu: HTTP polling API lịch sử (không dùng WebSocket nữa)

const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
const PORT = process.env.PORT || 3000;

// ===================================
// === CẤU HÌNH HỆ THỐNG ===
// ===================================
const CONFIG = {
    MAX_SESSIONS: 10000,
    MAX_PATTERN_HISTORY: 200,
    RECONNECT_DELAY: 2500,
    SAVE_INTERVAL: 30000,
    HISTORY_API_URL: "http://fi8.bot-hosting.net:20692/api/his",
    POLL_INTERVAL: 5000 // 5s/lần gọi API lấy lịch sử + phiên mới
};

// ===================================
// === TRẠNG THÁI HỆ THỐNG ===
// ===================================
const state = {
    apiResponse: {
        id: "@hadesdeton",
        phien: null,
        xuc_xac_1: null,
        xuc_xac_2: null,
        xuc_xac_3: null,
        tong: null,
        ket_qua: "",
        phien_hien_tai: null,
        du_doan: "?",
        pattern: "",
        so_sanh: "Đang chờ kết quả...",
        tong_so_phien: 0,
        so_lan_dung: 0,
        so_lan_sai: 0,
        ty_le_dung: "0%",
        do_tin_cay: "0%",
        thuat_toan: "Ensemble 30+"
    },
    currentSessionId: null,
    lastProcessedSessionId: null,
    patternHistory: [],
    detailedHistory: [], // Lịch sử chi tiết với total score
    sessionPredictions: new Map(),
    collectedSessions: new Map(),
    totalCorrect: 0,
    totalWrong: 0,
    algorithmPerformance: {},
    modelPredictions: { trend: {}, short: {}, mean: {}, switch: {}, bridge: {} }
};

// ===================================
// === PATTERN DATA TỪ pre.js ===
// ===================================
const PATTERN_DATA = {
    "tttt": {"tai": 73, "xiu": 27}, "xxxx": {"tai": 27, "xiu": 73},
    "tttttt": {"tai": 83, "xiu": 17}, "xxxxxx": {"tai": 17, "xiu": 83},
    "ttttx": {"tai": 40, "xiu": 60}, "xxxxt": {"tai": 60, "xiu": 40},
    "ttttttx": {"tai": 30, "xiu": 70}, "xxxxxxt": {"tai": 70, "xiu": 30},
    "ttxx": {"tai": 62, "xiu": 38}, "xxtt": {"tai": 38, "xiu": 62},
    "ttxxtt": {"tai": 32, "xiu": 68}, "xxttxx": {"tai": 68, "xiu": 32},
    "txx": {"tai": 60, "xiu": 40}, "xtt": {"tai": 40, "xiu": 60},
    "txxtx": {"tai": 63, "xiu": 37}, "xttxt": {"tai": 37, "xiu": 63},
    "tttxt": {"tai": 60, "xiu": 40}, "xxxtx": {"tai": 40, "xiu": 60},
    "tttxx": {"tai": 60, "xiu": 40}, "xxxtt": {"tai": 40, "xiu": 60},
    "txxt": {"tai": 60, "xiu": 40}, "xttx": {"tai": 40, "xiu": 60},
    "ttxxttx": {"tai": 30, "xiu": 70}, "xxttxxt": {"tai": 70, "xiu": 30},
    "tttttttt": {"tai": 88, "xiu": 12}, "xxxxxxxx": {"tai": 12, "xiu": 88},
    "tttttttx": {"tai": 25, "xiu": 75}, "xxxxxxxxt": {"tai": 75, "xiu": 25},
    "tttttxxx": {"tai": 35, "xiu": 65}, "xxxxtttt": {"tai": 65, "xiu": 35},
    "ttttxxxx": {"tai": 30, "xiu": 70}, "xxxxtttx": {"tai": 70, "xiu": 30},
    "txtxtx": {"tai": 68, "xiu": 32}, "xtxtxt": {"tai": 32, "xiu": 68},
    "ttxtxt": {"tai": 55, "xiu": 45}, "xxtxtx": {"tai": 45, "xiu": 55},
    "txtxxt": {"tai": 60, "xiu": 40}, "xtxttx": {"tai": 40, "xiu": 60},
    "ttx": {"tai": 65, "xiu": 35}, "xxt": {"tai": 35, "xiu": 65},
    "txt": {"tai": 58, "xiu": 42}, "xtx": {"tai": 42, "xiu": 58},
    "tttx": {"tai": 70, "xiu": 30}, "xxxt": {"tai": 30, "xiu": 70},
    "ttxt": {"tai": 63, "xiu": 37}, "xxtx": {"tai": 37, "xiu": 63},
    "txxx": {"tai": 25, "xiu": 75}, "xttt": {"tai": 75, "xiu": 25},
    "ttxtx": {"tai": 62, "xiu": 38}, "xxtxt": {"tai": 38, "xiu": 62},
    "ttxxt": {"tai": 55, "xiu": 45}, "xxttx": {"tai": 45, "xiu": 55},
    "tttttx": {"tai": 30, "xiu": 70}, "xxxxxt": {"tai": 70, "xiu": 30},
    "tttttttx": {"tai": 20, "xiu": 80}, "xxxxxxxt": {"tai": 80, "xiu": 20},
    "ttttttttx": {"tai": 15, "xiu": 85}, "xxxxxxxxt": {"tai": 85, "xiu": 15},
    "txtx": {"tai": 52, "xiu": 48}, "xtxt": {"tai": 48, "xiu": 52},
    "txtxt": {"tai": 53, "xiu": 47}, "xtxtx": {"tai": 47, "xiu": 53},
    "txtxtxt": {"tai": 57, "xiu": 43}, "xtxtxtx": {"tai": 43, "xiu": 57},
    "ttxxttxx": {"tai": 38, "xiu": 62}, "xxttxxtt": {"tai": 62, "xiu": 38},
    "ttxxxttx": {"tai": 45, "xiu": 55}, "xxttxxxt": {"tai": 55, "xiu": 45},
    "ttxtxttx": {"tai": 50, "xiu": 50}, "xxtxtxxt": {"tai": 50, "xiu": 50},
    "ttxttx": {"tai": 60, "xiu": 40}, "xxtxxt": {"tai": 40, "xiu": 60},
    "ttxxtx": {"tai": 58, "xiu": 42}, "xxtxxt": {"tai": 42, "xiu": 58},
    "ttxtxtx": {"tai": 62, "xiu": 38}, "xxtxtxt": {"tai": 38, "xiu": 62},
    "ttxxtxt": {"tai": 55, "xiu": 45}, "xxtxttx": {"tai": 45, "xiu": 55},
    "ttxtxxt": {"tai": 65, "xiu": 35}, "xxtxttx": {"tai": 35, "xiu": 65},
    "ttxtxttx": {"tai": 70, "xiu": 30}, "xxtxtxxt": {"tai": 30, "xiu": 70},
    "ttxxtxtx": {"tai": 68, "xiu": 32}, "xxtxtxtx": {"tai": 32, "xiu": 68},
    "ttxtxxtx": {"tai": 72, "xiu": 28}, "xxtxtxxt": {"tai": 28, "xiu": 72},
    "ttxxtxxt": {"tai": 75, "xiu": 25}, "xxtxtxxt": {"tai": 25, "xiu": 75},
};

const BIG_STREAK_DATA = {
    "tai": {
        "3": {"next_tai": 65, "next_xiu": 35},
        "4": {"next_tai": 70, "next_xiu": 30},
        "5": {"next_tai": 75, "next_xiu": 25},
        "6": {"next_tai": 80, "next_xiu": 20},
        "7": {"next_tai": 85, "next_xiu": 15},
        "8": {"next_tai": 88, "next_xiu": 12},
        "9": {"next_tai": 90, "next_xiu": 10},
        "10+": {"next_tai": 92, "next_xiu": 8}
    },
    "xiu": {
        "3": {"next_tai": 35, "next_xiu": 65},
        "4": {"next_tai": 30, "next_xiu": 70},
        "5": {"next_tai": 25, "next_xiu": 75},
        "6": {"next_tai": 20, "next_xiu": 80},
        "7": {"next_tai": 15, "next_xiu": 85},
        "8": {"next_tai": 12, "next_xiu": 88},
        "9": {"next_tai": 10, "next_xiu": 90},
        "10+": {"next_tai": 8, "next_xiu": 92}
    }
};

const SUM_STATS = {
    "3-10": {"tai": 0, "xiu": 100},
    "11": {"tai": 15, "xiu": 85},
    "12": {"tai": 25, "xiu": 75},
    "13": {"tai": 40, "xiu": 60},
    "14": {"tai": 50, "xiu": 50},
    "15": {"tai": 60, "xiu": 40},
    "16": {"tai": 75, "xiu": 25},
    "17": {"tai": 85, "xiu": 15},
    "18": {"tai": 100, "xiu": 0}
};

// ===================================
// === LỚP CƠ SỞ CHO THUẬT TOÁN ===
// ===================================
class BaseAlgorithm {
    constructor(name) {
        this.name = name;
        this.accuracy = 0;
        this.totalPredictions = 0;
        this.correctPredictions = 0;
        this.weight = 1.0;
        this.lastPrediction = null;
    }

    updateAccuracy(isCorrect) {
        this.totalPredictions++;
        if (isCorrect) this.correctPredictions++;
        this.accuracy = this.totalPredictions > 0 ? 
            this.correctPredictions / this.totalPredictions : 0;
    }

    getAccuracy() {
        return this.accuracy;
    }
}

// ===================================
// === 1. MARKOV CHAIN NÂNG CAO ===
// ===================================
class MarkovChainAlgorithm extends BaseAlgorithm {
    constructor() {
        super('MarkovChain');
        this.order = 5;
        this.transitionMatrix = {};
        this.confidenceThreshold = 0.6;
    }

    buildTransitionMatrix(history) {
        this.transitionMatrix = {};
        for (let order = 1; order <= this.order; order++) {
            for (let i = 0; i < history.length - order; i++) {
                const state = history.slice(i, i + order).join('');
                const next = history[i + order];
                if (!this.transitionMatrix[state]) {
                    this.transitionMatrix[state] = { T: 0, X: 0, total: 0 };
                }
                this.transitionMatrix[state][next]++;
                this.transitionMatrix[state].total++;
            }
        }
    }

    predict(history) {
        if (history.length < this.order) {
            return { prediction: 'Tài', confidence: 30, reason: 'Chưa đủ dữ liệu' };
        }
        this.buildTransitionMatrix(history);
        let bestPrediction = null;
        let bestConfidence = 0;
        let bestReason = '';
        for (let order = this.order; order >= 1; order--) {
            const currentState = history.slice(-order).join('');
            const transition = this.transitionMatrix[currentState];
            if (transition && transition.total >= 3) {
                const taiProb = transition.T / transition.total;
                const xiuProb = transition.X / transition.total;
                const confidence = Math.abs(taiProb - 0.5) * 200;
                if (confidence > bestConfidence) {
                    bestPrediction = taiProb > xiuProb ? 'Tài' : 'Xỉu';
                    bestConfidence = confidence;
                    bestReason = `Markov bậc ${order}: ${currentState} → ${bestPrediction} (${transition.total} lần)`;
                }
            }
        }
        if (!bestPrediction) {
            const totalTai = history.filter(h => h === 'T').length;
            const prob = totalTai / history.length;
            bestPrediction = prob > 0.5 ? 'Tài' : 'Xỉu';
            bestConfidence = Math.abs(prob - 0.5) * 200;
            bestReason = 'Phân tích toàn cục';
        }
        this.lastPrediction = bestPrediction;
        return { prediction: bestPrediction, confidence: Math.min(95, bestConfidence), reason: bestReason };
    }
}

// ===================================
// === 2. MẠNG NƠ-RON NHÂN TẠO (ANN) ===
// ===================================
class NeuralNetworkAlgorithm extends BaseAlgorithm {
    constructor() {
        super('NeuralNetwork');
        this.inputSize = 20;
        this.hiddenSize = 10;
        this.outputSize = 2;
        this.learningRate = 0.01;
        this.weightsIH = this.initWeights(this.inputSize, this.hiddenSize);
        this.weightsHO = this.initWeights(this.hiddenSize, this.outputSize);
        this.biasH = new Array(this.hiddenSize).fill(0).map(() => Math.random() * 0.1);
        this.biasO = new Array(this.outputSize).fill(0).map(() => Math.random() * 0.1);
    }

    initWeights(rows, cols) {
        return Array(rows).fill(0).map(() => 
            Array(cols).fill(0).map(() => Math.random() * 2 - 1)
        );
    }

    sigmoid(x) { return 1 / (1 + Math.exp(-x)); }
    sigmoidDerivative(x) { return x * (1 - x); }

    forward(input) {
        const hidden = this.biasH.map((bias, i) => {
            const sum = input.reduce((acc, val, j) => acc + val * this.weightsIH[j][i], 0);
            return this.sigmoid(sum + bias);
        });
        const output = this.biasO.map((bias, i) => {
            const sum = hidden.reduce((acc, val, j) => acc + val * this.weightsHO[j][i], 0);
            return this.sigmoid(sum + bias);
        });
        return { hidden, output };
    }

    train(input, target) {
        const { hidden, output } = this.forward(input);
        const outputErrors = target.map((t, i) => t - output[i]);
        const outputGradients = output.map((o, i) => 
            outputErrors[i] * this.sigmoidDerivative(o) * this.learningRate
        );
        for (let i = 0; i < this.hiddenSize; i++) {
            for (let j = 0; j < this.outputSize; j++) {
                this.weightsHO[i][j] += hidden[i] * outputGradients[j];
            }
        }
        const hiddenErrors = this.biasH.map((_, i) => {
            return this.weightsHO[i].reduce((sum, w, j) => sum + w * outputErrors[j], 0);
        });
        const hiddenGradients = hidden.map((h, i) => 
            hiddenErrors[i] * this.sigmoidDerivative(h) * this.learningRate
        );
        for (let i = 0; i < this.inputSize; i++) {
            for (let j = 0; j < this.hiddenSize; j++) {
                this.weightsIH[i][j] += input[i] * hiddenGradients[j];
            }
        }
        this.biasO = this.biasO.map((b, i) => b + outputGradients[i]);
        this.biasH = this.biasH.map((b, i) => b + hiddenGradients[i]);
        return output;
    }

    prepareInput(history) {
        const input = new Array(this.inputSize).fill(0.5);
        const recent = history.slice(-this.inputSize);
        for (let i = 0; i < recent.length; i++) {
            input[this.inputSize - recent.length + i] = recent[i] === 'T' ? 1 : 0;
        }
        return input;
    }

    predict(history) {
        if (history.length < 10) {
            return { prediction: 'Tài', confidence: 30, reason: 'ANN: Chưa đủ dữ liệu' };
        }
        const input = this.prepareInput(history);
        const { output } = this.forward(input);
        const taiProb = output[0];
        const confidence = Math.abs(taiProb - 0.5) * 200;
        const prediction = taiProb > 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(90, confidence), reason: `ANN: Xác suất Tài=${(taiProb*100).toFixed(1)}%` };
    }

    learnFromResult(history, actualResult) {
        if (history.length < 10) return;
        const input = this.prepareInput(history);
        const target = actualResult === 'Tài' ? [1, 0] : [0, 1];
        for (let i = 0; i < 5; i++) { this.train(input, target); }
    }
}

// ===================================
// === 3. LSTM ĐƠN GIẢN ===
// ===================================
class LSTMAlgorithm extends BaseAlgorithm {
    constructor() {
        super('LSTM');
        this.memory = [];
        this.maxMemory = 50;
        this.forgetGate = 0.1;
        this.rememberGate = 0.9;
        this.cellState = 0.5;
    }

    updateMemory(value) {
        this.memory.push(value);
        if (this.memory.length > this.maxMemory) this.memory.shift();
        this.cellState = this.cellState * (1 - this.forgetGate) + value * this.rememberGate;
    }

    predict(history) {
        if (history.length < 20) {
            return { prediction: 'Tài', confidence: 30, reason: 'LSTM: Đang khởi tạo' };
        }
        const recentValues = history.slice(-30).map(h => h === 'T' ? 1 : -1);
        const ma5 = this.movingAverage(recentValues, 5);
        const ma10 = this.movingAverage(recentValues, 10);
        const ma20 = this.movingAverage(recentValues, 20);
        const signal = (ma5 * 0.4 + ma10 * 0.35 + ma20 * 0.25);
        const lastValue = recentValues[recentValues.length - 1];
        this.updateMemory(lastValue);
        const smoothed = this.memory.reduce((a, b) => a + b, 0) / this.memory.length;
        const finalSignal = signal * 0.6 + smoothed * 0.4;
        const prediction = finalSignal > 0 ? 'Tài' : 'Xỉu';
        const confidence = Math.min(88, Math.abs(finalSignal) * 50 + 40);
        this.lastPrediction = prediction;
        return { prediction, confidence, reason: `LSTM: MA(5,10,20)=${ma5.toFixed(2)},${ma10.toFixed(2)},${ma20.toFixed(2)}` };
    }

    movingAverage(values, period) {
        if (values.length < period) return 0;
        const slice = values.slice(-period);
        return slice.reduce((a, b) => a + b, 0) / period;
    }
}

// ===================================
// === 4. THUẬT TOÁN DI TRUYỀN ===
// ===================================
class GeneticAlgorithm extends BaseAlgorithm {
    constructor() {
        super('Genetic');
        this.populationSize = 50;
        this.generations = 10;
        this.mutationRate = 0.1;
        this.population = [];
        this.initializePopulation();
    }

    initializePopulation() {
        this.population = [];
        for (let i = 0; i < this.populationSize; i++) {
            this.population.push({
                weights: {
                    trend: Math.random(), recent: Math.random(), balance: Math.random(),
                    streak: Math.random(), cycle: Math.random()
                },
                fitness: 0
            });
        }
    }

    fitness(individual, history) {
        let correct = 0, total = 0;
        for (let i = 20; i < history.length; i++) {
            const slice = history.slice(0, i);
            const prediction = this.predictWithWeights(slice, individual.weights);
            if (prediction === (history[i] === 'T' ? 'Tài' : 'Xỉu')) correct++;
            total++;
        }
        return total > 0 ? correct / total : 0;
    }

    predictWithWeights(history, weights) {
        if (history.length < 5) return 'Tài';
        let score = 0;
        const last10 = history.slice(-10);
        const taiRatio = last10.filter(h => h === 'T').length / last10.length;
        score += (taiRatio - 0.5) * 2 * weights.trend;
        const last3 = history.slice(-3);
        const recentTai = last3.filter(h => h === 'T').length / 3;
        score += (recentTai - 0.5) * 2 * weights.recent;
        const totalTai = history.filter(h => h === 'T').length;
        const globalRatio = totalTai / history.length;
        score += (0.5 - globalRatio) * 2 * weights.balance;
        let streak = 0;
        const lastResult = history[history.length - 1];
        for (let i = history.length - 1; i >= 0; i--) {
            if (history[i] === lastResult) streak++; else break;
        }
        score += (lastResult === 'T' ? -1 : 1) * (streak / 10) * weights.streak;
        const cycles = this.detectCycles(history);
        score += cycles * weights.cycle;
        return score > 0 ? 'Tài' : 'Xỉu';
    }

    detectCycles(history) {
        if (history.length < 10) return 0;
        const recent = history.slice(-20).join('');
        let cycles = 0;
        for (let period = 2; period <= 5; period++) {
            const pattern = recent.slice(-period);
            const before = recent.slice(-period * 2, -period);
            if (pattern === before) cycles += 1;
        }
        return cycles / 4 - 0.5;
    }

    evolve(history) {
        for (const individual of this.population) {
            individual.fitness = this.fitness(individual, history);
        }
        this.population.sort((a, b) => b.fitness - a.fitness);
        const elite = this.population.slice(0, 10);
        const newPopulation = [...elite];
        while (newPopulation.length < this.populationSize) {
            const parent1 = elite[Math.floor(Math.random() * elite.length)];
            const parent2 = elite[Math.floor(Math.random() * elite.length)];
            const child = { weights: {}, fitness: 0 };
            for (const key in parent1.weights) {
                child.weights[key] = Math.random() < 0.5 ? parent1.weights[key] : parent2.weights[key];
                if (Math.random() < this.mutationRate) {
                    child.weights[key] += (Math.random() - 0.5) * 0.2;
                    child.weights[key] = Math.max(0, Math.min(1, child.weights[key]));
                }
            }
            newPopulation.push(child);
        }
        this.population = newPopulation;
    }

    predict(history) {
        if (history.length < 20) {
            return { prediction: 'Tài', confidence: 30, reason: 'GA: Đang khởi tạo quần thể' };
        }
        this.evolve(history);
        const best = this.population[0];
        const prediction = this.predictWithWeights(history, best.weights);
        const confidence = Math.min(85, best.fitness * 100 + 30);
        this.lastPrediction = prediction;
        return { prediction, confidence, reason: `GA: Fitness=${(best.fitness*100).toFixed(1)}%` };
    }
}

// ===================================
// === 5. BAYESIAN INFERENCE ===
// ===================================
class BayesianAlgorithm extends BaseAlgorithm {
    constructor() {
        super('Bayesian');
        this.priorTai = 0.5;
        this.likelihoods = {
            afterTai: { T: 0, X: 0, total: 0 },
            afterXiu: { T: 0, X: 0, total: 0 },
            afterTT: { T: 0, X: 0, total: 0 },
            afterXX: { T: 0, X: 0, total: 0 },
            afterTX: { T: 0, X: 0, total: 0 },
            afterXT: { T: 0, X: 0, total: 0 }
        };
    }

    updateLikelihoods(history) {
        for (const key in this.likelihoods) {
            this.likelihoods[key] = { T: 0, X: 0, total: 0 };
        }
        for (let i = 1; i < history.length; i++) {
            const prev = history[i - 1];
            const curr = history[i];
            this.likelihoods[`after${prev === 'T' ? 'Tai' : 'Xiu'}`][curr]++;
            this.likelihoods[`after${prev === 'T' ? 'Tai' : 'Xiu'}`].total++;
            if (i >= 2) {
                const prev2 = history.slice(i - 2, i).join('');
                const state = `after${prev2}`;
                if (this.likelihoods[state]) {
                    this.likelihoods[state][curr]++;
                    this.likelihoods[state].total++;
                }
            }
        }
        const totalTai = history.filter(h => h === 'T').length;
        this.priorTai = totalTai / history.length;
    }

    predict(history) {
        if (history.length < 10) {
            return { prediction: 'Tài', confidence: 30, reason: 'Bayes: Khởi tạo' };
        }
        this.updateLikelihoods(history);
        const last1 = history[history.length - 1];
        const last2 = history.slice(-2).join('');
        let probTai = this.priorTai;
        const state1 = `after${last1 === 'T' ? 'Tai' : 'Xiu'}`;
        if (this.likelihoods[state1].total > 0) {
            const likeTai = this.likelihoods[state1].T / this.likelihoods[state1].total;
            probTai = (likeTai * this.priorTai) / (likeTai * this.priorTai + (1 - likeTai) * (1 - this.priorTai));
        }
        const state2 = `after${last2}`;
        if (this.likelihoods[state2] && this.likelihoods[state2].total > 3) {
            const likeTai2 = this.likelihoods[state2].T / this.likelihoods[state2].total;
            probTai = probTai * 0.5 + likeTai2 * 0.5;
        }
        const prediction = probTai > 0.5 ? 'Tài' : 'Xỉu';
        const confidence = Math.abs(probTai - 0.5) * 200;
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(92, confidence), reason: `Bayes: P(Tài)=${(probTai*100).toFixed(1)}%` };
    }
}

// ===================================
// === 6. RANDOM FOREST ===
// ===================================
class RandomForestAlgorithm extends BaseAlgorithm {
    constructor() {
        super('RandomForest');
        this.numTrees = 10;
        this.trees = [];
        this.initializeTrees();
    }

    initializeTrees() {
        this.trees = [];
        for (let i = 0; i < this.numTrees; i++) {
            this.trees.push({
                lookback: Math.floor(Math.random() * 15) + 5,
                threshold: Math.random() * 0.3 + 0.35,
                feature: ['trend', 'recent', 'streak', 'balance'][Math.floor(Math.random() * 4)]
            });
        }
    }

    treePredict(tree, history) {
        if (history.length < tree.lookback) return 'Tài';
        const slice = history.slice(-tree.lookback);
        let signal = 0;
        switch (tree.feature) {
            case 'trend':
                const firstHalf = slice.slice(0, Math.floor(slice.length / 2));
                const secondHalf = slice.slice(Math.floor(slice.length / 2));
                const firstTai = firstHalf.filter(h => h === 'T').length / firstHalf.length;
                const secondTai = secondHalf.filter(h => h === 'T').length / secondHalf.length;
                signal = secondTai - firstTai;
                break;
            case 'recent':
                const recent3 = slice.slice(-3);
                signal = recent3.filter(h => h === 'T').length / 3 - 0.5;
                break;
            case 'streak':
                let streak = 0;
                const last = slice[slice.length - 1];
                for (let i = slice.length - 1; i >= 0; i--) {
                    if (slice[i] === last) streak++; else break;
                }
                signal = (last === 'T' ? -1 : 1) * streak / 10;
                break;
            case 'balance':
                const taiRatio = slice.filter(h => h === 'T').length / slice.length;
                signal = 0.5 - taiRatio;
                break;
        }
        return signal > tree.threshold - 0.5 ? 'Tài' : 'Xỉu';
    }

    predict(history) {
        if (history.length < 10) {
            return { prediction: 'Tài', confidence: 30, reason: 'RF: Khởi tạo' };
        }
        const votes = { 'Tài': 0, 'Xỉu': 0 };
        for (const tree of this.trees) {
            const pred = this.treePredict(tree, history);
            votes[pred]++;
        }
        const prediction = votes['Tài'] > votes['Xỉu'] ? 'Tài' : 'Xỉu';
        const majority = Math.max(votes['Tài'], votes['Xỉu']);
        const confidence = (majority / this.numTrees) * 100;
        this.lastPrediction = prediction;
        return { prediction, confidence, reason: `RF: ${votes['Tài']}/${votes['Xỉu']} (${this.numTrees} cây)` };
    }
}

// ===================================
// === 7. TECHNICAL ANALYSIS ===
// ===================================
class TechnicalAnalysisAlgorithm extends BaseAlgorithm {
    constructor() {
        super('Technical');
    }

    calculateRSI(history, period = 14) {
        if (history.length < period + 1) return 50;
        const values = history.slice(-period - 1).map(h => h === 'T' ? 1 : 0);
        let gains = 0, losses = 0;
        for (let i = 1; i < values.length; i++) {
            const diff = values[i] - values[i - 1];
            if (diff > 0) gains += diff; else losses -= diff;
        }
        gains /= period; losses /= period;
        if (losses === 0) return 100;
        const rs = gains / losses;
        return 100 - (100 / (1 + rs));
    }

    calculateMACD(history) {
        if (history.length < 26) return 0;
        const values = history.map(h => h === 'T' ? 1 : -1);
        const ema12 = this.ema(values, 12);
        const ema26 = this.ema(values, 26);
        return ema12 - ema26;
    }

    ema(values, period) {
        const k = 2 / (period + 1);
        let ema = values[0];
        for (let i = 1; i < values.length; i++) {
            ema = values[i] * k + ema * (1 - k);
        }
        return ema;
    }

    calculateBollingerBands(history, period = 20) {
        if (history.length < period) return { upper: 0.6, lower: 0.4 };
        const values = history.slice(-period).map(h => h === 'T' ? 1 : 0);
        const mean = values.reduce((a, b) => a + b, 0) / period;
        const variance = values.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / period;
        const std = Math.sqrt(variance);
        return { upper: mean + 2 * std, lower: mean - 2 * std, mean };
    }

    predict(history) {
        if (history.length < 30) {
            return { prediction: 'Tài', confidence: 30, reason: 'TA: Cần thêm dữ liệu' };
        }
        const rsi = this.calculateRSI(history);
        const macd = this.calculateMACD(history);
        const bb = this.calculateBollingerBands(history);
        const lastValue = history[history.length - 1] === 'T' ? 1 : 0;
        let signals = [];
        if (rsi > 70) signals.push({ direction: 'Xỉu', strength: (rsi - 70) / 30 });
        else if (rsi < 30) signals.push({ direction: 'Tài', strength: (30 - rsi) / 30 });
        else signals.push({ direction: rsi > 50 ? 'Tài' : 'Xỉu', strength: Math.abs(rsi - 50) / 50 });
        if (Math.abs(macd) > 0.1) {
            signals.push({ direction: macd > 0 ? 'Tài' : 'Xỉu', strength: Math.min(1, Math.abs(macd) * 2) });
        }
        if (lastValue > bb.upper) signals.push({ direction: 'Xỉu', strength: 0.8 });
        else if (lastValue < bb.lower) signals.push({ direction: 'Tài', strength: 0.8 });
        let taiScore = 0, xiuScore = 0, totalStrength = 0;
        for (const signal of signals) {
            if (signal.direction === 'Tài') taiScore += signal.strength;
            else xiuScore += signal.strength;
            totalStrength += signal.strength;
        }
        const prediction = taiScore > xiuScore ? 'Tài' : 'Xỉu';
        const confidence = totalStrength > 0 ? Math.max(taiScore, xiuScore) / totalStrength * 100 : 50;
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(87, confidence), reason: `TA: RSI=${rsi.toFixed(0)}, MACD=${macd.toFixed(2)}` };
    }
}

// ===================================
// === 8. Q-LEARNING ===
// ===================================
class QLearningAlgorithm extends BaseAlgorithm {
    constructor() {
        super('QLearning');
        this.qTable = {};
        this.learningRate = 0.1;
        this.discountFactor = 0.95;
        this.explorationRate = 0.1;
    }

    getState(history) {
        if (history.length < 5) return 'initial';
        const last5 = history.slice(-5).join('');
        const taiCount = history.slice(-10).filter(h => h === 'T').length;
        const trend = taiCount >= 6 ? 'high' : taiCount <= 4 ? 'low' : 'mid';
        return `${last5}_${trend}`;
    }

    getQValue(state, action) {
        if (!this.qTable[state]) this.qTable[state] = { 'Tài': 0, 'Xỉu': 0 };
        return this.qTable[state][action];
    }

    updateQValue(state, action, reward, nextState) {
        const currentQ = this.getQValue(state, action);
        const nextMaxQ = Math.max(this.getQValue(nextState, 'Tài'), this.getQValue(nextState, 'Xỉu'));
        const newQ = currentQ + this.learningRate * (reward + this.discountFactor * nextMaxQ - currentQ);
        if (!this.qTable[state]) this.qTable[state] = { 'Tài': 0, 'Xỉu': 0 };
        this.qTable[state][action] = newQ;
    }

    predict(history) {
        const state = this.getState(history);
        if (Math.random() < this.explorationRate) {
            const prediction = Math.random() < 0.5 ? 'Tài' : 'Xỉu';
            this.lastPrediction = prediction;
            return { prediction, confidence: 40, reason: 'QL: Exploration' };
        }
        const taiQ = this.getQValue(state, 'Tài');
        const xiuQ = this.getQValue(state, 'Xỉu');
        const prediction = taiQ > xiuQ ? 'Tài' : 'Xỉu';
        const confidence = Math.min(85, Math.abs(taiQ - xiuQ) * 50 + 30);
        this.lastPrediction = prediction;
        return { prediction, confidence, reason: `QL: Q(Tài)=${taiQ.toFixed(2)}, Q(Xỉu)=${xiuQ.toFixed(2)}` };
    }

    learn(history, action, reward) {
        if (history.length < 5) return;
        const currentState = this.getState(history.slice(0, -1));
        const nextState = this.getState(history);
        this.updateQValue(currentState, action, reward, nextState);
    }
}

// ===================================
// === 9. PATTERN MATCHING NÂNG CAO ===
// ===================================
class PatternMatchingAlgorithm extends BaseAlgorithm {
    constructor() {
        super('PatternMatching');
        this.knownPatterns = new Map();
    }

    extractPatterns(history) {
        const patterns = {};
        for (let len = 3; len <= 7; len++) {
            for (let i = 0; i <= history.length - len - 1; i++) {
                const pattern = history.slice(i, i + len).join('');
                const next = history[i + len];
                if (!patterns[pattern]) patterns[pattern] = { T: 0, X: 0, total: 0 };
                patterns[pattern][next]++;
                patterns[pattern].total++;
            }
        }
        return patterns;
    }

    findBestMatch(history) {
        const patterns = this.extractPatterns(history);
        let bestMatch = null;
        let bestScore = -Infinity;
        for (let len = 7; len >= 3; len--) {
            const currentPattern = history.slice(-len).join('');
            const patternData = patterns[currentPattern];
            if (patternData && patternData.total >= 3) {
                const taiProb = patternData.T / patternData.total;
                const entropy = -taiProb * Math.log2(taiProb + 0.001) - (1 - taiProb) * Math.log2(1 - taiProb + 0.001);
                const score = patternData.total * (1 - entropy);
                if (score > bestScore) {
                    bestScore = score;
                    bestMatch = { pattern: currentPattern, data: patternData, length: len, occurrences: patternData.total };
                }
            }
        }
        return bestMatch;
    }

    predict(history) {
        if (history.length < 10) {
            return { prediction: 'Tài', confidence: 30, reason: 'PM: Cần thêm dữ liệu' };
        }
        const bestMatch = this.findBestMatch(history);
        if (!bestMatch || bestMatch.occurrences < 3) {
            const totalTai = history.filter(h => h === 'T').length;
            const prob = totalTai / history.length;
            const prediction = prob > 0.5 ? 'Tài' : 'Xỉu';
            this.lastPrediction = prediction;
            return { prediction, confidence: Math.abs(prob - 0.5) * 200, reason: 'PM: Fallback toàn cục' };
        }
        const taiProb = bestMatch.data.T / bestMatch.data.total;
        const prediction = taiProb > 0.5 ? 'Tài' : 'Xỉu';
        const confidence = Math.min(93, Math.abs(taiProb - 0.5) * 200 + bestMatch.occurrences * 2);
        this.lastPrediction = prediction;
        return { prediction, confidence, reason: `PM: "${bestMatch.pattern}" → ${prediction} (${bestMatch.occurrences} lần)` };
    }
}

// ===================================
// === 10. FOURIER TRANSFORM ===
// ===================================
class FourierAlgorithm extends BaseAlgorithm {
    constructor() {
        super('Fourier');
    }

    detectCycle(history) {
        if (history.length < 20) return 0;
        const values = history.map(h => h === 'T' ? 1 : -1);
        const n = values.length;
        let bestPeriod = 0;
        let bestCorrelation = -1;
        for (let period = 2; period <= Math.floor(n / 3); period++) {
            let correlation = 0;
            let count = 0;
            for (let i = 0; i < n - period; i++) {
                correlation += values[i] * values[i + period];
                count++;
            }
            correlation /= count;
            if (correlation > bestCorrelation) {
                bestCorrelation = correlation;
                bestPeriod = period;
            }
        }
        return { period: bestPeriod, correlation: bestCorrelation };
    }

    predict(history) {
        if (history.length < 30) {
            return { prediction: 'Tài', confidence: 30, reason: 'FFT: Cần thêm dữ liệu' };
        }
        const cycle = this.detectCycle(history);
        if (cycle.correlation < 0.3) {
            const last20 = history.slice(-20);
            const taiCount = last20.filter(h => h === 'T').length;
            const prediction = taiCount > 10 ? 'Tài' : 'Xỉu';
            this.lastPrediction = prediction;
            return { prediction, confidence: Math.abs(taiCount - 10) * 5 + 30, reason: 'FFT: Không phát hiện chu kỳ' };
        }
        const cycleLength = cycle.period;
        const position = history.length % cycleLength;
        const results = [];
        for (let i = position; i < history.length; i += cycleLength) {
            results.push(history[i]);
        }
        const taiCount = results.filter(r => r === 'T').length;
        const prediction = taiCount > results.length / 2 ? 'Tài' : 'Xỉu';
        const confidence = Math.min(82, Math.abs(taiCount / results.length - 0.5) * 200 + 30);
        this.lastPrediction = prediction;
        return { prediction, confidence, reason: `FFT: Chu kỳ=${cycleLength}, Tương quan=${cycle.correlation.toFixed(2)}` };
    }
}

// ===================================
// === THUẬT TOÁN MỚI TỪ predictionAlgorithms.js ===
// ===================================

// === 11. SMART BRIDGE BREAK ===
class SmartBridgeBreakAlgorithm extends BaseAlgorithm {
    constructor() {
        super('SmartBridge');
    }

    detectStreakAndBreak(history) {
        if (!history || history.length === 0) return { streak: 0, currentResult: null, breakProb: 0.0 };
        let streak = 1;
        const currentResult = history[history.length - 1];
        for (let i = history.length - 2; i >= 0; i--) {
            if (history[i] === currentResult) streak++; else break;
        }
        const last15 = history.slice(-15);
        if (!last15.length) return { streak, currentResult, breakProb: 0.0 };
        const switches = last15.slice(1).reduce((count, curr, idx) => count + (curr !== last15[idx] ? 1 : 0), 0);
        const taiCount = last15.filter(r => r === 'T').length;
        const xiuCount = last15.filter(r => r === 'X').length;
        const imbalance = Math.abs(taiCount - xiuCount) / last15.length;
        let breakProb = 0.0;
        if (streak >= 8) breakProb = Math.min(0.6 + (switches / 15) + imbalance * 0.15, 0.9);
        else if (streak >= 5) breakProb = Math.min(0.35 + (switches / 10) + imbalance * 0.25, 0.85);
        else if (streak >= 3 && switches >= 7) breakProb = 0.3;
        return { streak, currentResult, breakProb };
    }

    predict(history) {
        if (!history || history.length < 3) {
            return { prediction: 'Tài', confidence: 50, reason: 'SmartBridge: Không đủ dữ liệu' };
        }
        const { streak, currentResult, breakProb } = this.detectStreakAndBreak(history);
        let breakProbability = breakProb;
        if (streak >= 6) breakProbability = Math.min(breakProbability + 0.15, 0.9);
        else if (streak >= 4) breakProbability = Math.min(breakProbability + 0.1, 0.85);
        else breakProbability = Math.max(breakProbability - 0.15, 0.15);
        const prediction = breakProbability > 0.65 ? (currentResult === 'T' ? 'Xỉu' : 'Tài') : (currentResult === 'T' ? 'Tài' : 'Xỉu');
        this.lastPrediction = prediction;
        return {
            prediction,
            confidence: Math.min(85, breakProbability * 100),
            reason: `SmartBridge: Streak=${streak}, BreakProb=${(breakProbability*100).toFixed(0)}%`
        };
    }
}

// === 12. TREND AND PROBABILITY ===
class TrendAndProbAlgorithm extends BaseAlgorithm {
    constructor() {
        super('TrendProb');
    }

    predict(history) {
        if (!history || history.length < 3) return { prediction: 'Tài', confidence: 50, reason: 'TrendProb: Không đủ dữ liệu' };
        const last15 = history.slice(-15);
        if (!last15.length) return { prediction: 'Tài', confidence: 50, reason: 'TrendProb: Không đủ dữ liệu' };
        const weights = last15.map((_, i) => Math.pow(1.2, i));
        const taiWeighted = weights.reduce((sum, w, i) => sum + (last15[i] === 'T' ? w : 0), 0);
        const xiuWeighted = weights.reduce((sum, w, i) => sum + (last15[i] === 'X' ? w : 0), 0);
        const totalWeight = taiWeighted + xiuWeighted;
        const prediction = totalWeight > 0 && Math.abs(taiWeighted - xiuWeighted) / totalWeight >= 0.25 ?
            (taiWeighted > xiuWeighted ? 'Xỉu' : 'Tài') : (last15[last15.length - 1] === 'X' ? 'Tài' : 'Xỉu');
        this.lastPrediction = prediction;
        return {
            prediction,
            confidence: Math.min(80, Math.abs(taiWeighted - xiuWeighted) / (totalWeight || 1) * 100 + 30),
            reason: `TrendProb: Tài=${taiWeighted.toFixed(1)}, Xỉu=${xiuWeighted.toFixed(1)}`
        };
    }
}

// === 13. SHORT PATTERN ===
class ShortPatternAlgorithm extends BaseAlgorithm {
    constructor() {
        super('ShortPattern');
    }

    predict(history) {
        if (!history || history.length < 3) return { prediction: 'Tài', confidence: 50, reason: 'ShortPattern: Không đủ dữ liệu' };
        const { streak, currentResult, breakProb } = new SmartBridgeBreakAlgorithm().detectStreakAndBreak(history);
        if (streak >= 4) {
            const prediction = breakProb > 0.75 ? (currentResult === 'T' ? 'Xỉu' : 'Tài') : (currentResult === 'T' ? 'Tài' : 'Xỉu');
            this.lastPrediction = prediction;
            return { prediction, confidence: 70, reason: `ShortPattern: Streak=${streak}` };
        }
        const last8 = history.slice(-8);
        const prediction = last8[last8.length - 1] === 'X' ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: 55, reason: `ShortPattern: Đảo chiều` };
    }
}

// === 14. MEAN DEVIATION ===
class MeanDeviationAlgorithm extends BaseAlgorithm {
    constructor() {
        super('MeanDeviation');
    }

    predict(history) {
        if (!history || history.length < 3) return { prediction: 'Tài', confidence: 50, reason: 'MeanDev: Không đủ dữ liệu' };
        const last12 = history.slice(-12);
        if (!last12.length) return { prediction: 'Tài', confidence: 50, reason: 'MeanDev: Không đủ dữ liệu' };
        const taiCount = last12.filter(r => r === 'T').length;
        const xiuCount = last12.length - taiCount;
        const deviation = Math.abs(taiCount - xiuCount) / last12.length;
        const prediction = deviation < 0.35 ? (last12[last12.length - 1] === 'X' ? 'Tài' : 'Xỉu') : (xiuCount > taiCount ? 'Tài' : 'Xỉu');
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(75, deviation * 100 + 40), reason: `MeanDev: Độ lệch=${(deviation*100).toFixed(0)}%` };
    }
}

// === 15. RECENT SWITCH ===
class RecentSwitchAlgorithm extends BaseAlgorithm {
    constructor() {
        super('RecentSwitch');
    }

    predict(history) {
        if (!history || history.length < 3) return { prediction: 'Tài', confidence: 50, reason: 'RecentSwitch: Không đủ dữ liệu' };
        const last10 = history.slice(-10);
        if (!last10.length) return { prediction: 'Tài', confidence: 50, reason: 'RecentSwitch: Không đủ dữ liệu' };
        const switches = last10.slice(1).reduce((count, curr, idx) => count + (curr !== last10[idx] ? 1 : 0), 0);
        const prediction = switches >= 6 ? (last10[last10.length - 1] === 'X' ? 'Tài' : 'Xỉu') : (last10[last10.length - 1] === 'X' ? 'Tài' : 'Xỉu');
        this.lastPrediction = prediction;
        return { prediction, confidence: 60, reason: `RecentSwitch: Switches=${switches}` };
    }
}

// === 16. AI HTDD LOGIC ===
class AIHTDDAlgorithm extends BaseAlgorithm {
    constructor() {
        super('AI_HTDD');
    }

    predict(history) {
        if (!history || history.length < 3) {
            const pred = Math.random() < 0.5 ? 'Tài' : 'Xỉu';
            this.lastPrediction = pred;
            return { prediction: pred, confidence: 50, reason: 'AI HTDD: Ngẫu nhiên' };
        }
        const recentHistory = history.slice(-5);
        const taiCount = recentHistory.filter(r => r === 'T').length;
        const xiuCount = recentHistory.filter(r => r === 'X').length;

        if (history.length >= 3) {
            const last3 = history.slice(-3);
            if (last3.join(',') === 'T,X,T') {
                this.lastPrediction = 'Xỉu';
                return { prediction: 'Xỉu', confidence: 75, reason: 'AI HTDD: Mẫu 1T1X → Xỉu' };
            } else if (last3.join(',') === 'X,T,X') {
                this.lastPrediction = 'Tài';
                return { prediction: 'Tài', confidence: 75, reason: 'AI HTDD: Mẫu 1X1T → Tài' };
            }
        }

        if (history.length >= 9 && history.slice(-6).every(h => h === 'T')) {
            this.lastPrediction = 'Xỉu';
            return { prediction: 'Xỉu', confidence: 85, reason: 'AI HTDD: Chuỗi Tài dài → Xỉu' };
        } else if (history.length >= 9 && history.slice(-6).every(h => h === 'X')) {
            this.lastPrediction = 'Tài';
            return { prediction: 'Tài', confidence: 85, reason: 'AI HTDD: Chuỗi Xỉu dài → Tài' };
        }

        if (taiCount > xiuCount + 1) {
            this.lastPrediction = 'Xỉu';
            return { prediction: 'Xỉu', confidence: 65, reason: 'AI HTDD: Tài nhiều → Xỉu' };
        } else if (xiuCount > taiCount + 1) {
            this.lastPrediction = 'Tài';
            return { prediction: 'Tài', confidence: 65, reason: 'AI HTDD: Xỉu nhiều → Tài' };
        }

        const overallTai = history.filter(h => h === 'T').length;
        const overallXiu = history.filter(h => h === 'X').length;
        if (overallTai > overallXiu + 2) {
            this.lastPrediction = 'Xỉu';
            return { prediction: 'Xỉu', confidence: 60, reason: 'AI HTDD: Tổng Tài nhiều → Xỉu' };
        } else if (overallXiu > overallTai + 2) {
            this.lastPrediction = 'Tài';
            return { prediction: 'Tài', confidence: 60, reason: 'AI HTDD: Tổng Xỉu nhiều → Tài' };
        }

        const pred = Math.random() < 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = pred;
        return { prediction: pred, confidence: 50, reason: 'AI HTDD: Cân bằng, ngẫu nhiên' };
    }
}

// === 17. PATTERN ANALYSIS TỪ pre.js ===
class PatternAnalysisAlgorithm extends BaseAlgorithm {
    constructor() {
        super('PatternAnalysis');
    }

    findClosestPattern(inputPatternOldestFirst) {
        if (!inputPatternOldestFirst) return null;
        const keys = Object.keys(PATTERN_DATA).sort((a, b) => b.length - a.length);
        for (const key of keys) {
            if (inputPatternOldestFirst.endsWith(key)) return key;
        }
        return null;
    }

    analyzeBigStreak(history) {
        if (history.length < 2) return { prediction: null, confidence: 0 };
        let currentStreak = 1;
        const currentResult = history[0];
        for (let i = 1; i < history.length; i++) {
            if (history[i] === currentResult) currentStreak++;
            else break;
        }
        if (currentStreak >= 3) {
            const streakKey = currentStreak <= 9 ? String(currentStreak) : "10+";
            const resultKey = currentResult === 'T' ? 'tai' : 'xiu';
            const stats = BIG_STREAK_DATA[resultKey]?.[streakKey];
            if (stats) {
                return {
                    prediction: stats.next_tai > stats.next_xiu ? 'Tài' : 'Xỉu',
                    confidence: Math.max(stats.next_tai, stats.next_xiu)
                };
            }
        }
        return { prediction: null, confidence: 0 };
    }

    predict(history) {
        if (!history || history.length === 0) {
            return { prediction: 'Tài', confidence: 50, reason: 'PA: Mặc định' };
        }

        // Phân tích cầu lớn trước
        const streakResult = this.analyzeBigStreak(history);
        if (streakResult.prediction && streakResult.confidence > 75) {
            this.lastPrediction = streakResult.prediction;
            return { prediction: streakResult.prediction, confidence: streakResult.confidence, reason: 'PA: Cầu lớn' };
        }

        // Phân tích pattern
        const elements = history.slice(0, 15);
        const currentPatternStr = elements.join('');
        const closestPatternKey = this.findClosestPattern(currentPatternStr);

        if (closestPatternKey) {
            const data = PATTERN_DATA[closestPatternKey];
            const prediction = data.tai > data.xiu ? 'Tài' : 'Xỉu';
            const confidence = Math.max(data.tai, data.xiu);
            this.lastPrediction = prediction;
            return { prediction, confidence, reason: `PA: Pattern "${closestPatternKey}"` };
        }

        // Fallback
        const prediction = elements[0] === 'X' ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: 55, reason: 'PA: Fallback' };
    }
}

// ===================================
// === 18-30. THUẬT TOÁN BỔ SUNG NÂNG CAO ===
// ===================================
class MonteCarloAlgorithm extends BaseAlgorithm {
    constructor() { super('MonteCarlo'); }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'MC: Khởi tạo' };
        let simulations = 1000;
        let taiWins = 0;
        const taiProb = history.filter(h => h === 'T').length / history.length;
        for (let i = 0; i < simulations; i++) {
            if (Math.random() < taiProb) taiWins++;
        }
        const prediction = taiWins > simulations / 2 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(taiWins / simulations - 0.5) * 200, reason: `MC: ${simulations} mô phỏng` };
    }
}

class KNNAlgorithm extends BaseAlgorithm {
    constructor() { super('KNN'); this.k = 5; }
    predict(history) {
        if (history.length < 5) return { prediction: 'Tài', confidence: 50, reason: 'KNN: Khởi tạo' };
        const lastPattern = history.slice(-5).join('');
        const distances = [];
        for (let i = 0; i <= history.length - 6; i++) {
            const pattern = history.slice(i, i + 5).join('');
            let distance = 0;
            for (let j = 0; j < 5; j++) {
                if (pattern[j] !== lastPattern[j]) distance++;
            }
            const nextResult = history[i + 5];
            distances.push({ distance, result: nextResult });
        }
        distances.sort((a, b) => a.distance - b.distance);
        const nearest = distances.slice(0, this.k);
        const taiCount = nearest.filter(n => n.result === 'T').length;
        const prediction = taiCount > this.k / 2 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(taiCount / this.k - 0.5) * 200 + 40, reason: `KNN: ${taiCount}/${this.k}` };
    }
}

class SVMAlgorithm extends BaseAlgorithm {
    constructor() { super('SVM'); }
    predict(history) {
        if (history.length < 20) return { prediction: 'Tài', confidence: 50, reason: 'SVM: Khởi tạo' };
        const features = [];
        for (let i = 10; i < history.length; i++) {
            const slice = history.slice(i - 10, i);
            const taiCount = slice.filter(h => h === 'T').length;
            features.push({ x: taiCount / 10, label: history[i] === 'T' ? 1 : -1 });
        }
        const w = features.reduce((sum, f) => sum + f.x * f.label, 0) / features.length;
        const lastTaiCount = history.slice(-10).filter(h => h === 'T').length / 10;
        const prediction = lastTaiCount * w > 0 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(80, Math.abs(lastTaiCount * w) * 100 + 30), reason: `SVM: w=${w.toFixed(3)}` };
    }
}

class GradientBoostingAlgorithm extends BaseAlgorithm {
    constructor() { super('GradientBoost'); this.models = []; }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'GB: Khởi tạo' };
        let prediction = 0;
        const features = [history.slice(-5).filter(h => h === 'T').length / 5, history.slice(-10).filter(h => h === 'T').length / 10, history.length > 0 ? (history[history.length - 1] === 'T' ? 1 : 0) : 0.5];
        for (const feature of features) {
            prediction += (feature - 0.5) * 0.33;
        }
        const finalPrediction = prediction > 0 ? 'Tài' : 'Xỉu';
        this.lastPrediction = finalPrediction;
        return { prediction: finalPrediction, confidence: Math.min(75, Math.abs(prediction) * 100 + 30), reason: 'GB: Ensemble yếu' };
    }
}

class ARIMAAlgorithm extends BaseAlgorithm {
    constructor() { super('ARIMA'); }
    predict(history) {
        if (history.length < 15) return { prediction: 'Tài', confidence: 50, reason: 'ARIMA: Khởi tạo' };
        const values = history.slice(-15).map(h => h === 'T' ? 1 : 0);
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        const recentTrend = values.slice(-3).reduce((a, b) => a + b, 0) / 3;
        const prediction = (mean * 0.4 + recentTrend * 0.6) > 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(mean * 0.4 + recentTrend * 0.6 - 0.5) * 200, reason: `ARIMA: Mean=${mean.toFixed(2)}` };
    }
}

class FuzzyLogicAlgorithm extends BaseAlgorithm {
    constructor() { super('FuzzyLogic'); }
    predict(history) {
        if (history.length < 5) return { prediction: 'Tài', confidence: 50, reason: 'Fuzzy: Khởi tạo' };
        const last5 = history.slice(-5);
        const taiCount = last5.filter(h => h === 'T').length;
        let membership = 0;
        if (taiCount >= 4) membership = 0.8;
        else if (taiCount >= 3) membership = 0.6;
        else if (taiCount >= 2) membership = 0.4;
        else membership = 0.2;
        const prediction = membership > 0.5 ? 'Xỉu' : 'Tài';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(membership - 0.5) * 200 + 30, reason: `Fuzzy: Membership=${membership.toFixed(2)}` };
    }
}

class ChaosTheoryAlgorithm extends BaseAlgorithm {
    constructor() { super('ChaosTheory'); }
    predict(history) {
        if (history.length < 20) return { prediction: 'Tài', confidence: 50, reason: 'Chaos: Khởi tạo' };
        const lyapunovExponent = this.calculateLyapunov(history);
        const prediction = lyapunovExponent > 0 ? (history[history.length - 1] === 'T' ? 'Xỉu' : 'Tài') : (history[history.length - 1] === 'T' ? 'Tài' : 'Xỉu');
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(70, Math.abs(lyapunovExponent) * 50 + 40), reason: `Chaos: LE=${lyapunovExponent.toFixed(3)}` };
    }
    calculateLyapunov(history) {
        let sum = 0;
        const values = history.map(h => h === 'T' ? 1 : -1);
        for (let i = 1; i < Math.min(20, values.length); i++) {
            sum += Math.abs(values[i] - values[i - 1]);
        }
        return sum / Math.min(20, values.length - 1) - 0.5;
    }
}

class WaveletAlgorithm extends BaseAlgorithm {
    constructor() { super('Wavelet'); }
    predict(history) {
        if (history.length < 16) return { prediction: 'Tài', confidence: 50, reason: 'Wavelet: Khởi tạo' };
        const values = history.slice(-16).map(h => h === 'T' ? 1 : -1);
        const approx = [];
        const detail = [];
        for (let i = 0; i < values.length; i += 2) {
            approx.push((values[i] + values[i + 1]) / 2);
            detail.push((values[i] - values[i + 1]) / 2);
        }
        const prediction = approx[approx.length - 1] > 0 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(70, Math.abs(approx[approx.length - 1]) * 50 + 30), reason: `Wavelet: Approx=${approx[approx.length-1].toFixed(2)}` };
    }
}

class KalmanFilterAlgorithm extends BaseAlgorithm {
    constructor() { super('KalmanFilter'); this.estimate = 0.5; this.errorCovariance = 0.1; this.processNoise = 0.01; this.measurementNoise = 0.1; }
    predict(history) {
        if (history.length < 5) return { prediction: 'Tài', confidence: 50, reason: 'Kalman: Khởi tạo' };
        for (const result of history.slice(-10)) {
            const measurement = result === 'T' ? 1 : 0;
            this.errorCovariance += this.processNoise;
            const kalmanGain = this.errorCovariance / (this.errorCovariance + this.measurementNoise);
            this.estimate = this.estimate + kalmanGain * (measurement - this.estimate);
            this.errorCovariance = (1 - kalmanGain) * this.errorCovariance;
        }
        const prediction = this.estimate > 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(this.estimate - 0.5) * 200, reason: `Kalman: Estimate=${this.estimate.toFixed(3)}` };
    }
}

class ParticleSwarmAlgorithm extends BaseAlgorithm {
    constructor() { super('ParticleSwarm'); this.particles = Array(20).fill(0).map(() => ({ position: Math.random(), velocity: (Math.random() - 0.5) * 0.1, best: Math.random() })); this.globalBest = 0.5; }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'PSO: Khởi tạo' };
        const targetRatio = history.filter(h => h === 'T').length / history.length;
        for (const particle of this.particles) {
            if (Math.abs(particle.position - targetRatio) < Math.abs(particle.best - targetRatio)) {
                particle.best = particle.position;
            }
            if (Math.abs(particle.best - targetRatio) < Math.abs(this.globalBest - targetRatio)) {
                this.globalBest = particle.best;
            }
            particle.velocity = particle.velocity * 0.7 + (particle.best - particle.position) * 0.15 + (this.globalBest - particle.position) * 0.15;
            particle.position += particle.velocity;
            particle.position = Math.max(0, Math.min(1, particle.position));
        }
        const prediction = this.globalBest > 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(this.globalBest - 0.5) * 200, reason: `PSO: GlobalBest=${this.globalBest.toFixed(3)}` };
    }
}

class AdaBoostAlgorithm extends BaseAlgorithm {
    constructor() { super('AdaBoost'); }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'AdaBoost: Khởi tạo' };
        let score = 0;
        const patterns = [
            () => history.slice(-3).filter(h => h === 'T').length >= 2 ? 1 : -1,
            () => history.slice(-5).filter(h => h === 'T').length >= 3 ? 1 : -1,
            () => history[history.length - 1] === 'T' ? -1 : 1,
            () => history.filter(h => h === 'T').length > history.length / 2 ? -1 : 1
        ];
        const weights = [0.3, 0.25, 0.25, 0.2];
        for (let i = 0; i < patterns.length; i++) {
            score += patterns[i]() * weights[i];
        }
        const prediction = score > 0 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(80, Math.abs(score) * 50 + 30), reason: `AdaBoost: Score=${score.toFixed(2)}` };
    }
}

class HiddenMarkovModelAlgorithm extends BaseAlgorithm {
    constructor() { super('HMM'); this.states = ['Bullish', 'Bearish', 'Neutral']; this.transitionProb = { 'Bullish': { 'Bullish': 0.6, 'Bearish': 0.2, 'Neutral': 0.2 }, 'Bearish': { 'Bullish': 0.2, 'Bearish': 0.6, 'Neutral': 0.2 }, 'Neutral': { 'Bullish': 0.25, 'Bearish': 0.25, 'Neutral': 0.5 } }; this.currentState = 'Neutral'; }
    predict(history) {
        if (history.length < 5) return { prediction: 'Tài', confidence: 50, reason: 'HMM: Khởi tạo' };
        const last3Tai = history.slice(-3).filter(h => h === 'T').length;
        if (last3Tai >= 2) this.currentState = 'Bullish';
        else if (last3Tai <= 1) this.currentState = 'Bearish';
        else this.currentState = 'Neutral';
        const probs = this.transitionProb[this.currentState];
        const rand = Math.random();
        let nextState;
        if (rand < probs.Bullish) nextState = 'Bullish';
        else if (rand < probs.Bullish + probs.Bearish) nextState = 'Bearish';
        else nextState = 'Neutral';
        const prediction = nextState === 'Bullish' ? 'Tài' : nextState === 'Bearish' ? 'Xỉu' : (Math.random() < 0.5 ? 'Tài' : 'Xỉu');
        this.lastPrediction = prediction;
        return { prediction, confidence: 60, reason: `HMM: State=${this.currentState}→${nextState}` };
    }
}

class EnsembleVotingAlgorithm extends BaseAlgorithm {
    constructor() { super('EnsembleVoting'); }
    predict(history) {
        return { prediction: 'Tài', confidence: 50, reason: 'Voting: Placeholder' };
    }
}

class DeepQLearningAlgorithm extends BaseAlgorithm {
    constructor() { super('DeepQ'); this.qValues = {}; }
    predict(history) {
        if (history.length < 5) return { prediction: 'Tài', confidence: 50, reason: 'DeepQ: Khởi tạo' };
        const state = history.slice(-5).join('');
        if (!this.qValues[state]) this.qValues[state] = { 'Tài': 0, 'Xỉu': 0 };
        const prediction = this.qValues[state]['Tài'] > this.qValues[state]['Xỉu'] ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(this.qValues[state]['Tài'] - this.qValues[state]['Xỉu']) * 10 + 40, reason: `DeepQ: State=${state}` };
    }
}

class StochasticGradientAlgorithm extends BaseAlgorithm {
    constructor() { super('SGD'); this.weight = 0.5; this.bias = 0; }
    predict(history) {
        if (history.length < 5) return { prediction: 'Tài', confidence: 50, reason: 'SGD: Khởi tạo' };
        const x = history.slice(-5).filter(h => h === 'T').length / 5;
        const output = this.weight * x + this.bias;
        const prediction = output > 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(output - 0.5) * 200, reason: `SGD: Output=${output.toFixed(3)}` };
    }
    learn(history, actualResult) {
        if (history.length < 5) return;
        const x = history.slice(-5).filter(h => h === 'T').length / 5;
        const y = actualResult === 'Tài' ? 1 : 0;
        const output = this.weight * x + this.bias;
        const error = y - output;
        this.weight += 0.01 * error * x;
        this.bias += 0.01 * error;
    }
}

class XGBoostAlgorithm extends BaseAlgorithm {
    constructor() { super('XGBoost'); }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'XGBoost: Khởi tạo' };
        let score = 0;
        const trees = [
            (h) => h.slice(-3).filter(r => r === 'T').length >= 2 ? 0.3 : -0.3,
            (h) => h.slice(-5).filter(r => r === 'T').length >= 3 ? 0.25 : -0.25,
            (h) => h[h.length - 1] === 'T' ? -0.2 : 0.2,
            (h) => h.filter(r => r === 'T').length > h.length / 2 ? -0.15 : 0.15,
            (h) => h.slice(-10).filter(r => r === 'T').length >= 6 ? 0.2 : -0.2
        ];
        for (const tree of trees) {
            score += tree(history);
        }
        const prediction = score > 0 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(85, Math.abs(score) * 100 + 30), reason: `XGBoost: Score=${score.toFixed(3)}` };
    }
}

class CatBoostAlgorithm extends BaseAlgorithm {
    constructor() { super('CatBoost'); }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'CatBoost: Khởi tạo' };
        const categorical = history.map(h => h === 'T' ? 1 : 0);
        const smoothed = categorical.reduce((a, b) => a + b, 0) / categorical.length;
        const prediction = smoothed > 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(smoothed - 0.5) * 200, reason: `CatBoost: Smoothed=${smoothed.toFixed(3)}` };
    }
}

class LightGBMAlgorithm extends BaseAlgorithm {
    constructor() { super('LightGBM'); }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'LightGBM: Khởi tạo' };
        const gradient = history.slice(-10).filter(h => h === 'T').length / 10 - history.slice(-20, -10).filter(h => h === 'T').length / 10;
        const prediction = gradient > 0 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.min(80, Math.abs(gradient) * 200 + 30), reason: `LightGBM: Gradient=${gradient.toFixed(3)}` };
    }
}

class AutoEncoderAlgorithm extends BaseAlgorithm {
    constructor() { super('AutoEncoder'); this.encoded = 0.5; }
    predict(history) {
        if (history.length < 10) return { prediction: 'Tài', confidence: 50, reason: 'AE: Khởi tạo' };
        const input = history.slice(-10).map(h => h === 'T' ? 1 : 0).reduce((a, b) => a + b, 0) / 10;
        this.encoded = input * 0.7 + this.encoded * 0.3;
        const prediction = this.encoded > 0.5 ? 'Tài' : 'Xỉu';
        this.lastPrediction = prediction;
        return { prediction, confidence: Math.abs(this.encoded - 0.5) * 200, reason: `AE: Encoded=${this.encoded.toFixed(3)}` };
    }
}

// ===================================
// === ENSEMBLE MANAGER 30+ THUẬT TOÁN ===
// ===================================
class EnsembleManager {
    constructor() {
        this.algorithms = [
            new MarkovChainAlgorithm(),
            new NeuralNetworkAlgorithm(),
            new LSTMAlgorithm(),
            new GeneticAlgorithm(),
            new BayesianAlgorithm(),
            new RandomForestAlgorithm(),
            new TechnicalAnalysisAlgorithm(),
            new QLearningAlgorithm(),
            new PatternMatchingAlgorithm(),
            new FourierAlgorithm(),
            new SmartBridgeBreakAlgorithm(),
            new TrendAndProbAlgorithm(),
            new ShortPatternAlgorithm(),
            new MeanDeviationAlgorithm(),
            new RecentSwitchAlgorithm(),
            new AIHTDDAlgorithm(),
            new PatternAnalysisAlgorithm(),
            new MonteCarloAlgorithm(),
            new KNNAlgorithm(),
            new SVMAlgorithm(),
            new GradientBoostingAlgorithm(),
            new ARIMAAlgorithm(),
            new FuzzyLogicAlgorithm(),
            new ChaosTheoryAlgorithm(),
            new WaveletAlgorithm(),
            new KalmanFilterAlgorithm(),
            new ParticleSwarmAlgorithm(),
            new AdaBoostAlgorithm(),
            new HiddenMarkovModelAlgorithm(),
            new DeepQLearningAlgorithm(),
            new StochasticGradientAlgorithm(),
            new XGBoostAlgorithm(),
            new CatBoostAlgorithm(),
            new LightGBMAlgorithm(),
            new AutoEncoderAlgorithm()
        ];
;
        
        this.algorithmWeights = this.algorithms.map(() => 1);
        this.performanceWindow = 50;
    }

    updateWeights() {
        const totalAccuracy = this.algorithms.reduce((sum, algo) => sum + algo.getAccuracy(), 0);
        if (totalAccuracy > 0) {
            this.algorithms.forEach((algo, i) => {
                this.algorithmWeights[i] = algo.getAccuracy() / totalAccuracy;
                algo.weight = this.algorithmWeights[i];
            });
        }
    }

    predict(history) {
        const predictions = [];
        const details = [];
        
        for (const algorithm of this.algorithms) {
            try {
                const result = algorithm.predict(history);
                predictions.push({
                    prediction: result.prediction,
                    confidence: result.confidence,
                    weight: algorithm.weight,
                    name: algorithm.name,
                    reason: result.reason
                });
                details.push(`${algorithm.name}: ${result.prediction} (${result.confidence.toFixed(0)}%)`);
            } catch (error) {
                console.error(`[❌] Lỗi thuật toán ${algorithm.name}:`, error.message);
            }
        }
        
        let taiScore = 0, xiuScore = 0;
        
        for (const pred of predictions) {
            const weightedConfidence = pred.confidence * pred.weight;
            if (pred.prediction === 'Tài') {
                taiScore += weightedConfidence;
            } else {
                xiuScore += weightedConfidence;
            }
        }
        
        const finalPrediction = taiScore > xiuScore ? 'Tài' : 'Xỉu';
        const totalScore = taiScore + xiuScore;
        const finalConfidence = totalScore > 0 ? 
            Math.max(taiScore, xiuScore) / totalScore * 100 : 50;
        
        const taiVotes = predictions.filter(p => p.prediction === 'Tài').length;
        const xiuVotes = predictions.filter(p => p.prediction === 'Xỉu').length;
        
        const bestAlgo = [...this.algorithms].sort((a, b) => b.getAccuracy() - a.getAccuracy())[0];
        
        return {
            prediction: finalPrediction,
            confidence: finalConfidence,
            votes: { tai: taiVotes, xiu: xiuVotes },
            totalAlgorithms: predictions.length,
            bestAlgorithm: bestAlgo ? bestAlgo.name : 'Unknown',
            details: details,
            weightedScores: { tai: taiScore.toFixed(2), xiu: xiuScore.toFixed(2) }
        };
    }

    learnFromResult(history, actualResult) {
        for (const algorithm of this.algorithms) {
            if (algorithm.lastPrediction) {
                const isCorrect = algorithm.lastPrediction === actualResult;
                algorithm.updateAccuracy(isCorrect);
            }
        }
        
        const nn = this.algorithms.find(a => a instanceof NeuralNetworkAlgorithm);
        if (nn) {
            nn.learnFromResult(history, actualResult);
        }
        
        const ql = this.algorithms.find(a => a instanceof QLearningAlgorithm);
        if (ql && ql.lastPrediction) {
            const reward = ql.lastPrediction === actualResult ? 1 : -1;
            ql.learn(history, ql.lastPrediction, reward);
        }
        
        const sgd = this.algorithms.find(a => a instanceof StochasticGradientAlgorithm);
        if (sgd) {
            sgd.learn(history, actualResult);
        }
        
        this.updateWeights();
        
        for (const algorithm of this.algorithms) {
            state.algorithmPerformance[algorithm.name] = {
                accuracy: (algorithm.getAccuracy() * 100).toFixed(1) + '%',
                predictions: algorithm.totalPredictions,
                weight: algorithm.weight.toFixed(3)
            };
        }
    }

    getPerformanceReport() {
        return this.algorithms.map(algo => ({
            name: algo.name,
            accuracy: (algo.getAccuracy() * 100).toFixed(1) + '%',
            totalPredictions: algo.totalPredictions,
            correctPredictions: algo.correctPredictions,
            weight: algo.weight.toFixed(3)
        })).sort((a, b) => parseFloat(b.accuracy) - parseFloat(a.accuracy));
    }
}

// ===================================
// === KHỞI TẠO ENSEMBLE MANAGER ===
// ===================================
const ensemble = new EnsembleManager();

// ===================================
// === QUẢN LÝ DỰ ĐOÁN ===
// ===================================
function getOrCreatePrediction(sessionId) {
    if (state.sessionPredictions.has(sessionId)) {
        return state.sessionPredictions.get(sessionId);
    }
    
    const historyForPrediction = state.patternHistory;
    const result = ensemble.predict(historyForPrediction);
    
    const predData = {
        prediction: result.prediction,
        confidence: result.confidence,
        timestamp: new Date().toISOString(),
        votes: result.votes,
        bestAlgorithm: result.bestAlgorithm,
        details: result.details
    };
    
    state.sessionPredictions.set(sessionId, predData);
    
    if (!state.collectedSessions.has(sessionId)) {
        state.collectedSessions.set(sessionId, {
            sessionId,
            prediction: result.prediction,
            confidence: result.confidence.toFixed(1),
            timestamp: new Date().toISOString(),
            result: null,
            isCorrect: null,
            algorithm: result.bestAlgorithm,
            votes: `${result.votes.tai}/${result.votes.xiu}`
        });
    }
    
    if (state.sessionPredictions.size > 10000) {
        const firstKey = state.sessionPredictions.keys().next().value;
        state.sessionPredictions.delete(firstKey);
    }
    
    console.log(`\n${'='.repeat(60)}`);
    console.log(`[🎯] ENSEMBLE DỰ ĐOÁN PHIÊN ${sessionId}`);
    console.log(`[📊] Kết quả: ${result.prediction} | Độ tin cậy: ${result.confidence.toFixed(1)}%`);
    console.log(`[🗳️] Votes: Tài=${result.votes.tai} | Xỉu=${result.votes.xiu} (${result.totalAlgorithms} thuật toán)`);
    console.log(`[🏆] Thuật toán tốt nhất: ${result.bestAlgorithm}`);
    console.log(`[📝] Top 5: ${result.details.slice(0, 5).join(', ')}`);
    console.log(`${'='.repeat(60)}\n`);
    
    return predData;
}

function handleNewSession(sessionId) {
    if (sessionId === state.lastProcessedSessionId) return null;
    
    console.log(`\n[🆔] PHIÊN MỚI: ${sessionId}`);
    state.lastProcessedSessionId = sessionId;
    
    const predData = getOrCreatePrediction(sessionId);
    
    Object.assign(state.apiResponse, {
        phien: sessionId,
        phien_hien_tai: (parseInt(sessionId) + 1).toString(),
        du_doan: predData.prediction,
        do_tin_cay: predData.confidence.toFixed(1) + '%',
        so_sanh: "Đang chờ kết quả mới...",
        tong_so_phien: state.collectedSessions.size,
        thuat_toan: predData.bestAlgorithm
    });
    
    return predData;
}

// ===================================
// === XỬ LÝ KẾT QUẢ ===
// ===================================
function processGameResult(sessionId, d1, d2, d3) {
    const total = d1 + d2 + d3;
    const result = total > 10 ? 'T' : 'X';
    const resultText = result === 'T' ? 'Tài' : 'Xỉu';
    
    state.patternHistory.push(result);
    if (state.patternHistory.length > CONFIG.MAX_PATTERN_HISTORY) {
        state.patternHistory.shift();
    }
    
    state.detailedHistory.push({
        session: sessionId,
        result: resultText,
        totalScore: total,
        dice1: d1,
        dice2: d2,
        dice3: d3,
        timestamp: new Date().toISOString()
    });
    if (state.detailedHistory.length > 200) {
        state.detailedHistory.shift();
    }
    
    const predData = getOrCreatePrediction(sessionId);
    const isCorrect = predData.prediction === resultText;
    
    if (isCorrect) {
        state.totalCorrect++;
    } else {
        state.totalWrong++;
    }
    
    if (state.collectedSessions.has(sessionId)) {
        const sessionData = state.collectedSessions.get(sessionId);
        sessionData.result = resultText;
        sessionData.isCorrect = isCorrect;
        sessionData.dice1 = d1;
        sessionData.dice2 = d2;
        sessionData.dice3 = d3;
        sessionData.total = total;
    }
    
    ensemble.learnFromResult(state.patternHistory, resultText);
    
    const totalPredictions = state.totalCorrect + state.totalWrong;
    const accuracy = totalPredictions > 0 ? ((state.totalCorrect / totalPredictions) * 100).toFixed(2) : 0;
    
    Object.assign(state.apiResponse, {
        xuc_xac_1: d1,
        xuc_xac_2: d2,
        xuc_xac_3: d3,
        tong: total,
        ket_qua: resultText,
        phien_hien_tai: (parseInt(sessionId) + 1).toString(),
        du_doan: predData.prediction,
        so_sanh: `Dự đoán: ${predData.prediction} | Kết quả: ${isCorrect ? '✅ ĐÚNG' : '❌ SAI'}`,
        pattern: state.patternHistory.slice(-50).join(''),
        tong_so_phien: state.collectedSessions.size,
        so_lan_dung: state.totalCorrect,
        so_lan_sai: state.totalWrong,
        ty_le_dung: `${accuracy}%`,
        do_tin_cay: predData.confidence.toFixed(1) + '%'
    });
    
    console.log(`\n${'~'.repeat(60)}`);
    console.log(`[🎲] KẾT QUẢ PHIÊN ${sessionId}: ${d1}-${d2}-${d3} = ${total} (${resultText})`);
    console.log(`[🎯] Dự đoán: ${predData.prediction} | ${isCorrect ? '✅ ĐÚNG' : '❌ SAI'}`);
    console.log(`[📊] Tổng: ${state.collectedSessions.size} phiên | Đúng: ${state.totalCorrect} | Sai: ${state.totalWrong} | Tỉ lệ: ${accuracy}%`);
    console.log(`${'~'.repeat(60)}\n`);
    
    if (state.collectedSessions.size % 50 === 0) {
        console.log('\n' + '='.repeat(80));
        console.log('[📈] BÁO CÁO HIỆU SUẤT TOP 15 THUẬT TOÁN:');
        console.log('='.repeat(80));
        const report = ensemble.getPerformanceReport().slice(0, 15);
        report.forEach((algo, i) => {
            const medal = i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : '  ';
            console.log(`${medal} ${algo.name.padEnd(20)} | Độ chính xác: ${algo.accuracy.padEnd(8)} | Dự đoán: ${algo.totalPredictions} | Trọng số: ${algo.weight}`);
        });
        console.log('='.repeat(80) + '\n');
    }
    
    if (state.collectedSessions.size % 100 === 0) {
        console.log(`\n[🎉] CỘT MỐC ${state.collectedSessions.size} PHIÊN! Độ chính xác: ${accuracy}%\n`);
        saveCollectedData();
    }
    
    return { resultText, isCorrect };
}

// ===================================
// === LƯU DỮ LIỆU ===
// ===================================
function saveCollectedData() {
    try {
        const totalPredictions = state.totalCorrect + state.totalWrong;
        const accuracy = totalPredictions > 0 ? 
            ((state.totalCorrect / totalPredictions) * 100).toFixed(2) : 0;
        
        const data = {
            totalSessions: state.collectedSessions.size,
            correctPredictions: state.totalCorrect,
            wrongPredictions: state.totalWrong,
            accuracy: accuracy + '%',
            patternHistory: state.patternHistory,
            algorithmPerformance: state.algorithmPerformance,
            algorithmReport: ensemble.getPerformanceReport(),
            sessions: Array.from(state.collectedSessions.values()).slice(-10000),
            lastUpdate: new Date().toISOString()
        };
        
        fs.writeFileSync(
            path.join(__dirname, 'ai_data.json'),
            JSON.stringify(data, null, 2)
        );
        
        const csvRows = ['SessionID,Timestamp,Prediction,Confidence,Algorithm,Votes,Result,IsCorrect,Dice1,Dice2,Dice3,Total'];
        Array.from(state.collectedSessions.values())
            .slice(-5000)
            .forEach(s => {
                csvRows.push(`${s.sessionId},${s.timestamp},${s.prediction},${s.confidence},${s.algorithm || ''},${s.votes || ''},${s.result || 'N/A'},${s.isCorrect || 'N/A'},${s.dice1 || ''},${s.dice2 || ''},${s.dice3 || ''},${s.total || ''}`);
            });
        
        fs.writeFileSync(
            path.join(__dirname, 'ai_predictions.csv'),
            csvRows.join('\n')
        );
        
        console.log(`[💾] Đã lưu ${state.collectedSessions.size} phiên. Độ chính xác: ${accuracy}%`);
    } catch (error) {
        console.error('[❌] Lỗi lưu dữ liệu:', error.message);
    }
}

// ===================================
// === NGUỒN DỮ LIỆU: HTTP POLLING API LỊCH SỬ ===
// ===================================
let pollTimer = null;
let isFetching = false;
let historyLoaded = false;
const processedSessionIds = new Set();

// Lấy field theo nhiều tên khả dĩ (không phân biệt hoa/thường)
function pickField(obj, keys) {
    for (const k of keys) {
        if (obj[k] !== undefined && obj[k] !== null) return obj[k];
    }
    const lowerMap = {};
    Object.keys(obj).forEach(key => { lowerMap[key.toLowerCase()] = obj[key]; });
    for (const k of keys) {
        const v = lowerMap[k.toLowerCase()];
        if (v !== undefined && v !== null) return v;
    }
    return undefined;
}

// Tìm mảng phiên trong response, dù API bọc trong data/list/history/...
function extractSessionsArray(body) {
    if (Array.isArray(body)) return body;
    if (body && typeof body === 'object') {
        const commonKeys = ['data', 'list', 'history', 'sessions', 'his', 'result', 'items', 'rows'];
        for (const key of commonKeys) {
            if (Array.isArray(body[key])) return body[key];
        }
        for (const key of Object.keys(body)) {
            if (Array.isArray(body[key])) return body[key];
        }
    }
    return [];
}

// Chuẩn hoá 1 phiên về {sessionId, d1, d2, d3, total}
function normalizeSessionItem(item) {
    if (!item || typeof item !== 'object') return null;

    const sessionIdRaw = pickField(item, ['Phien', 'phien', 'session', 'sessionId', 'Session_id', 'id', 'sid', 'Ky', 'ky', 'phien_hien_tai']);
    const d1Raw = pickField(item, ['Xuc_xac_1', 'xuc_xac_1', 'dice1', 'd1', 'Xuc_xac1', 'xx1']);
    const d2Raw = pickField(item, ['Xuc_xac_2', 'xuc_xac_2', 'dice2', 'd2', 'Xuc_xac2', 'xx2']);
    const d3Raw = pickField(item, ['Xuc_xac_3', 'xuc_xac_3', 'dice3', 'd3', 'Xuc_xac3', 'xx3']);
    const totalRaw = pickField(item, ['Tong', 'tong', 'total', 'sum', 'Sum']);

    const sessionId = sessionIdRaw !== undefined ? String(sessionIdRaw) : null;
    const d1 = d1Raw !== undefined ? parseInt(d1Raw) : null;
    const d2 = d2Raw !== undefined ? parseInt(d2Raw) : null;
    const d3 = d3Raw !== undefined ? parseInt(d3Raw) : null;
    let total = totalRaw !== undefined ? parseInt(totalRaw) : null;

    if (total === null && d1 !== null && d2 !== null && d3 !== null) {
        total = d1 + d2 + d3;
    }
    if (!sessionId || total === null || isNaN(total)) return null;

    return {
        sessionId,
        d1: d1 !== null && !isNaN(d1) ? d1 : 0,
        d2: d2 !== null && !isNaN(d2) ? d2 : 0,
        d3: d3 !== null && !isNaN(d3) ? d3 : 0,
        total
    };
}

// Lấy dữ liệu từ API + xử lý: nạp TOÀN BỘ lịch sử để ttoan học ngay,
// không chờ đủ số phiên mới bắt đầu dự đoán (dự đoán luôn ngay khi có dữ liệu).
async function fetchAndProcessHistory() {
    if (isFetching) return;
    isFetching = true;
    try {
        const res = await fetch(CONFIG.HISTORY_API_URL);
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const body = await res.json();
        const rawList = extractSessionsArray(body);

        if (!historyLoaded && rawList.length > 0) {
            console.log('[📥] Mẫu 1 phiên từ API (kiểm tra field):', JSON.stringify(rawList[0]));
        }

        const normalized = rawList.map(normalizeSessionItem).filter(Boolean);

        normalized.sort((a, b) => {
            const na = Number(a.sessionId), nb = Number(b.sessionId);
            if (!isNaN(na) && !isNaN(nb)) return na - nb;
            return a.sessionId.localeCompare(b.sessionId);
        });

        let newCount = 0;
        for (const item of normalized) {
            if (processedSessionIds.has(item.sessionId)) continue;

            handleNewSession(item.sessionId);
            processGameResult(item.sessionId, item.d1, item.d2, item.d3);

            processedSessionIds.add(item.sessionId);
            newCount++;
        }
        if (processedSessionIds.size > CONFIG.MAX_SESSIONS * 2) {
            const excess = processedSessionIds.size - CONFIG.MAX_SESSIONS * 2;
            const it = processedSessionIds.values();
            for (let i = 0; i < excess; i++) processedSessionIds.delete(it.next().value);
        }

        if (!historyLoaded) {
            historyLoaded = true;
            console.log(`[✅] Đã nạp ${normalized.length} phiên lịch sử. Ttoan bắt đầu dự đoán ngay, không cần chờ đủ 10k phiên.`);
        } else if (newCount > 0) {
            console.log(`[🆕] Đã xử lý ${newCount} phiên mới.`);
        }

        // Luôn đưa ra dự đoán cho phiên KẾ TIẾP (chưa có kết quả), hiển thị ngay qua /sunlon
        if (normalized.length > 0) {
            const lastId = normalized[normalized.length - 1].sessionId;
            const lastIdNum = Number(lastId);
            const nextId = !isNaN(lastIdNum) ? String(lastIdNum + 1) : `${lastId}_next`;
            if (nextId !== state.lastProcessedSessionId) {
                handleNewSession(nextId);
            }
        }
    } catch (err) {
        console.error('[❌] Lỗi lấy dữ liệu từ API:', err.message);
    } finally {
        isFetching = false;
    }
}

function startPolling() {
    fetchAndProcessHistory();
    clearInterval(pollTimer);
    pollTimer = setInterval(fetchAndProcessHistory, CONFIG.POLL_INTERVAL);
}

// ===================================
// === API ENDPOINTS ===
// ===================================
app.get('/sunlon', (req, res) => {
    res.json(state.apiResponse);
});

app.get('/data', (req, res) => {
    const limit = parseInt(req.query.limit) || 100;
    const offset = parseInt(req.query.offset) || 0;
    
    const sessions = Array.from(state.collectedSessions.values())
        .slice(offset, offset + limit);
    
    res.json({
        total: state.collectedSessions.size,
        correct: state.totalCorrect,
        wrong: state.totalWrong,
        accuracy: state.totalCorrect + state.totalWrong > 0 ? 
            ((state.totalCorrect / (state.totalCorrect + state.totalWrong)) * 100).toFixed(2) + '%' : '0%',
        sessions,
        patternHistory: state.patternHistory
    });
});

app.get('/stats', (req, res) => {
    const total = state.patternHistory.length;
    const taiCount = state.patternHistory.filter(p => p === 'T').length;
    const xiuCount = total - taiCount;
    
    const nextPrediction = getOrCreatePrediction(state.currentSessionId);
    const algorithmReport = ensemble.getPerformanceReport();
    
    res.json({
        totalSessions: state.collectedSessions.size,
        accuracy: state.totalCorrect + state.totalWrong > 0 ? 
            ((state.totalCorrect / (state.totalCorrect + state.totalWrong)) * 100).toFixed(2) + '%' : '0%',
        patternStats: {
            total,
            taiCount,
            xiuCount,
            taiPercent: total > 0 ? ((taiCount / total) * 100).toFixed(2) + '%' : '0%',
            xiuPercent: total > 0 ? ((xiuCount / total) * 100).toFixed(2) + '%' : '0%'
        },
        currentPrediction: nextPrediction,
        algorithmPerformance: state.algorithmPerformance,
        algorithmReport,
        lastUpdate: new Date().toISOString()
    });
});

app.get('/algorithms', (req, res) => {
    res.json({
        algorithms: ensemble.getPerformanceReport(),
        totalAlgorithms: ensemble.algorithms.length,
        currentWeights: ensemble.algorithms.map((algo, i) => ({
            name: algo.name,
            weight: ensemble.algorithmWeights[i]
        }))
    });
});

app.get('/export', (req, res) => {
    const csvRows = ['SessionID,Timestamp,Prediction,Confidence,Algorithm,Votes,Result,IsCorrect,Dice1,Dice2,Dice3,Total'];
    Array.from(state.collectedSessions.values())
        .forEach(s => {
            csvRows.push(`${s.sessionId},${s.timestamp},${s.prediction},${s.confidence},${s.algorithm || ''},${s.votes || ''},${s.result || 'N/A'},${s.isCorrect || 'N/A'},${s.dice1 || ''},${s.dice2 || ''},${s.dice3 || ''},${s.total || ''}`);
        });
    
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename=ai_ensemble_${state.collectedSessions.size}.csv`);
    res.send(csvRows.join('\n'));
});

app.get('/', (req, res) => {
    const totalPredictions = state.totalCorrect + state.totalWrong;
    const accuracy = totalPredictions > 0 ? 
        ((state.totalCorrect / totalPredictions) * 100).toFixed(2) : 0;
    
    const report = ensemble.getPerformanceReport();
    const topAlgorithms = report.slice(0, 15);
    
    res.send(`
        <!DOCTYPE html>
        <html>
        <head>
            <title>🤖 AI ENSEMBLE - 35 THUẬT TOÁN PRO</title>
            <meta charset="UTF-8">
            <meta http-equiv="refresh" content="8">
            <style>
                * { margin: 0; padding: 0; box-sizing: border-box; }
                body { 
                    font-family: 'Segoe UI', Arial, sans-serif; 
                    background: linear-gradient(135deg, #0f0c29, #302b63, #24243e);
                    min-height: 100vh; padding: 20px; color: white;
                }
                .container { max-width: 1200px; margin: 0 auto; }
                .header { 
                    background: rgba(255,255,255,0.1); backdrop-filter: blur(10px);
                    padding: 30px; border-radius: 20px; text-align: center; margin-bottom: 20px;
                    border: 1px solid rgba(255,255,255,0.2);
                }
                .header h1 { font-size: 2.5em; margin-bottom: 10px; background: linear-gradient(45deg, #ff6b6b, #4ecdc4, #45b7d1); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
                .ai-badge { display: inline-block; background: linear-gradient(45deg, #ff6b6b, #4ecdc4); color: white; padding: 8px 20px; border-radius: 25px; font-weight: bold; margin: 5px; font-size: 0.9em; }
                .prediction-box { 
                    font-size: 2.5em; font-weight: bold; padding: 30px; margin: 20px 0; border-radius: 20px; text-align: center;
                    animation: pulse 2s infinite; position: relative; overflow: hidden;
                }
                .prediction-box.tai { background: linear-gradient(135deg, #11998e, #38ef7d); }
                .prediction-box.xiu { background: linear-gradient(135deg, #eb3349, #f45c43); }
                @keyframes pulse {
                    0% { transform: scale(1); box-shadow: 0 0 20px rgba(255,255,255,0.3); }
                    50% { transform: scale(1.02); box-shadow: 0 0 40px rgba(255,255,255,0.5); }
                    100% { transform: scale(1); box-shadow: 0 0 20px rgba(255,255,255,0.3); }
                }
                .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 15px; margin: 20px 0; }
                .card { 
                    background: rgba(255,255,255,0.1); backdrop-filter: blur(10px);
                    padding: 20px; border-radius: 15px; text-align: center;
                    border: 1px solid rgba(255,255,255,0.2);
                }
                .card h3 { color: rgba(255,255,255,0.7); font-size: 0.9em; margin-bottom: 10px; }
                .card .value { font-size: 2em; font-weight: bold; }
                .card.success .value { color: #38ef7d; }
                .card.danger .value { color: #f45c43; }
                .card.info .value { color: #4ecdc4; }
                .card.warning .value { color: #ffd93d; }
                .algo-table { 
                    background: rgba(255,255,255,0.1); backdrop-filter: blur(10px);
                    border-radius: 15px; overflow: hidden; margin: 20px 0;
                    border: 1px solid rgba(255,255,255,0.2);
                }
                .algo-table table { width: 100%; border-collapse: collapse; font-size: 0.9em; }
                .algo-table th { 
                    background: rgba(255,255,255,0.2); padding: 10px; text-align: left;
                    font-weight: bold; color: #ffd93d;
                }
                .algo-table td { padding: 10px; border-top: 1px solid rgba(255,255,255,0.1); }
                .rank-1 { color: #ffd700; font-weight: bold; }
                .rank-2 { color: #c0c0c0; font-weight: bold; }
                .rank-3 { color: #cd7f32; font-weight: bold; }
                .links { display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; margin: 20px 0; }
                .links a { 
                    display: inline-block; padding: 12px 20px; background: rgba(255,255,255,0.2); 
                    color: white; text-decoration: none; border-radius: 25px; font-weight: bold; 
                    transition: all 0.3s; border: 1px solid rgba(255,255,255,0.3);
                }
                .links a:hover { transform: translateY(-2px); background: rgba(255,255,255,0.3); }
                .footer { text-align: center; margin-top: 20px; opacity: 0.7; font-size: 0.85em; }
                .algo-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 8px; margin: 10px 0; }
                .algo-tag { background: rgba(255,255,255,0.15); padding: 6px 12px; border-radius: 15px; font-size: 0.75em; text-align: center; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🤖 AI ENSEMBLE TÀI XỈU</h1>
                    <div>
                        <span class="ai-badge">🧠 35 Thuật Toán Pro</span>
                        <span class="ai-badge">🎯 Ensemble Learning</span>
                        <span class="ai-badge">🔄 Tự Học</span>
                        <span class="ai-badge">📊 ML/DL/AI</span>
                    </div>
                </div>
                
                <div class="prediction-box ${state.apiResponse.du_doan === 'Tài' ? 'tai' : 'xiu'}">
                    🎯 Dự đoán: <strong>${state.apiResponse.du_doan}</strong>
                    <div style="font-size: 0.5em; margin-top: 10px;">Độ tin cậy: ${state.apiResponse.do_tin_cay}</div>
                    <div style="font-size: 0.4em; opacity: 0.8;">Thuật toán tốt nhất: ${state.apiResponse.thuat_toan}</div>
                </div>
                
                <div class="grid">
                    <div class="card info">
                        <h3>📊 Tổng phiên</h3>
                        <div class="value">${state.collectedSessions.size}</div>
                    </div>
                    <div class="card success">
                        <h3>✅ Dự đoán đúng</h3>
                        <div class="value">${state.totalCorrect}</div>
                    </div>
                    <div class="card danger">
                        <h3>❌ Dự đoán sai</h3>
                        <div class="value">${state.totalWrong}</div>
                    </div>
                    <div class="card warning">
                        <h3>📈 Độ chính xác</h3>
                        <div class="value">${accuracy}%</div>
                    </div>
                    <div class="card info">
                        <h3>🧠 Thuật toán</h3>
                        <div class="value" style="font-size: 1.2em;">35</div>
                    </div>
                </div>
                
                <div class="algo-table">
                    <table>
                        <tr>
                            <th>#</th><th>Thuật toán</th><th>Độ chính xác</th><th>Dự đoán</th><th>Trọng số</th>
                        </tr>
                        ${topAlgorithms.map((algo, i) => `
                        <tr>
                            <td class="rank-${Math.min(i+1, 3)}">${i === 0 ? '🥇' : i === 1 ? '🥈' : i === 2 ? '🥉' : i+1}</td>
                            <td>${algo.name}</td>
                            <td><strong>${algo.accuracy}</strong></td>
                            <td>${algo.totalPredictions}</td>
                            <td>${algo.weight}</td>
                        </tr>
                        `).join('')}
                    </table>
                </div>
                
                <div class="card" style="text-align: left; padding: 15px; margin: 10px 0;">
                    <h3 style="color: #ffd93d; margin-bottom: 10px;">🧠 Danh sách 35 thuật toán:</h3>
                    <div class="algo-grid">
                        ${ensemble.algorithms.map(a => `<span class="algo-tag">${a.name}</span>`).join('')}
                    </div>
                </div>
                
                ${state.apiResponse.tong ? `
                <div class="card" style="text-align: left; padding: 20px;">
                    <h3 style="color: #ffd93d; margin-bottom: 15px;">🎲 Thông tin phiên gần nhất</h3>
                    <p>📍 Phiên: <strong>${state.apiResponse.phien}</strong></p>
                    <p>🎲 Xúc xắc: ${state.apiResponse.xuc_xac_1} - ${state.apiResponse.xuc_xac_2} - ${state.apiResponse.xuc_xac_3}</p>
                    <p>📊 Tổng: <strong>${state.apiResponse.tong}</strong> (${state.apiResponse.ket_qua})</p>
                    <p>🎯 ${state.apiResponse.so_sanh}</p>
                </div>
                ` : ''}
                
                <div class="links">
                    <a href="/sunlon">📡 API JSON</a>
                    <a href="/data">📊 Dữ liệu</a>
                    <a href="/stats">📈 Thống kê</a>
                    <a href="/algorithms">🧠 Thuật toán</a>
                    <a href="/export">💾 Xuất CSV</a>
                    <a href="#" onclick="location.reload()">🔄 Làm mới</a>
                </div>
                
                <div class="footer">
                    <p>🚀 Hệ thống Ensemble kết hợp 35 thuật toán AI/ML/DL hàng đầu</p>
                    <p>📊 Markov Chain | Neural Network | LSTM | Genetic Algorithm | Bayesian | Random Forest | Technical Analysis | Q-Learning | Pattern Matching | Fourier Transform | Smart Bridge | Trend Prob | Short Pattern | Mean Deviation | Recent Switch | AI HTDD | Pattern Analysis | Monte Carlo | KNN | SVM | Gradient Boost | ARIMA | Fuzzy Logic | Chaos Theory | Wavelet | Kalman Filter | Particle Swarm | AdaBoost | HMM | Deep Q | SGD | XGBoost | CatBoost | LightGBM | AutoEncoder</p>
                </div>
            </div>
        </body>
        </html>
    `);
});

// ===================================
// === KHỞI ĐỘNG ===
// ===================================
app.listen(PORT, () => {
    console.log(`
╔══════════════════════════════════════════════════════╗
║                                                      ║
║   🤖 AI ENSEMBLE TÀI XỈU - 35 THUẬT TOÁN PRO       ║
║                                                      ║
║   📊 Markov Chain (Bậc 5)                           ║
║   🧠 Neural Network (ANN 3 lớp)                     ║
║   📈 LSTM (Long Short-Term Memory)                  ║
║   🧬 Genetic Algorithm                              ║
║   📐 Bayesian Inference                             ║
║   🌲 Random Forest (10 cây)                         ║
║   📉 Technical Analysis (RSI, MACD, BB)            ║
║   🎮 Q-Learning (Reinforcement)                     ║
║   🔍 Pattern Matching (Nâng cao)                    ║
║   🌊 Fourier Transform (Chu kỳ)                     ║
║   🌉 Smart Bridge Break                             ║
║   📊 Trend & Probability                            ║
║   📐 Short Pattern                                  ║
║   📏 Mean Deviation                                 ║
║   🔄 Recent Switch                                  ║
║   🤖 AI HTDD Logic                                  ║
║   📋 Pattern Analysis (Sunwin)                      ║
║   🎲 Monte Carlo Simulation                         ║
║   👥 K-Nearest Neighbors                            ║
║   📐 Support Vector Machine                         ║
║   🚀 Gradient Boosting                              ║
║   📈 ARIMA Time Series                              ║
║   🧠 Fuzzy Logic                                    ║
║   🌪️ Chaos Theory                                   ║
║   🌊 Wavelet Transform                              ║
║   🎯 Kalman Filter                                  ║
║   🐦 Particle Swarm Optimization                    ║
║   💪 AdaBoost                                       ║
║   🔗 Hidden Markov Model                            ║
║   🧠 Deep Q-Learning                                ║
║   📉 Stochastic Gradient Descent                    ║
║   🚀 XGBoost                                        ║
║   🐱 CatBoost                                       ║
║   💡 LightGBM                                       ║
║   🔄 AutoEncoder                                    ║
║                                                      ║
║   🌐 Server: http://localhost:${PORT}                   ║
║   📊 Dashboard: http://localhost:${PORT}                ║
║                                                      ║
╚══════════════════════════════════════════════════════╝
    `);
    startPolling();
});

// Xử lý tắt server
process.on('SIGINT', () => {
    console.log('\n[🛑] Đang tắt server, lưu dữ liệu...');
    saveCollectedData();
    process.exit();
});

process.on('SIGTERM', () => {
    console.log('\n[🛑] Đang tắt server, lưu dữ liệu...');
    saveCollectedData();
    process.exit();
});

// Lưu định kỳ
setInterval(saveCollectedData, CONFIG.SAVE_INTERVAL);