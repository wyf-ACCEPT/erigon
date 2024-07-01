"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = __importDefault(require("fs"));
const ethers_1 = require("ethers");
const lossless_json_1 = require("lossless-json");
// import {JSON} from "core-js/modules/esnext.json.raw-json";
const JSONbigAlways = require('json-bigint')({
    useNativeBigInt: true,
    //alwaysParseAsBig: true
});
//
// class jsonBig implements JSON {
//
//     readonly [Symbol.toStringTag]: string = JSON[Symbol.toStringTag]
//
//     parse(text: string, reviver?: (this: any, key: string, value: any) => any): any {
//         return parse(text, reviver)
//     }
//
//     stringify(value: any, replacer?: (this: any, key: string, value: any) => any, space?: string | number): string;
//     stringify(value: any, replacer?: (number | string)[] | null, space?: string | number): string;
//     stringify(value: any, replacer?: ((this: any, key: string, value: any) => any) | (number | string)[] | null, space?: string | number): string {
//         return <string>stringify(value, replacer, space)
//     }
// }
//
// JSON.parse =parse
// // @ts-ignore
// JSON.stringify = stringify
console.info(JSON);
console.info(JSON.rawJson);
console.log(JSON.stringify);
console.log(JSON.parse);
// JSONbig.parse()
const _parse = lossless_json_1.parse;
const _stringify = lossless_json_1.stringify;
const _originalParse = JSON.parse;
const _originalStringify = JSON.stringify;
JSON.parse = function (text, reviver) {
    console.log("wtf");
    // return _parse(text, reviver)
    // return JSONbig.parse(text, reviver)
    return JSONbigAlways.parse(text, reviver);
};
JSON.stringify = function (value, replacer, space) {
    console.log("wtf stringify");
    // const res = _stringify(value, replacer, space)
    // if (!res) {
    //     return ""
    // }
    // return res
    // return JSONbig.stringify(value, null, space)
    const xxx = function bigintReplacer(key, value) {
        if (typeof value === "bigint") {
            console.log("bigint value");
            console.log(value);
            console.log((0, ethers_1.toNumber)(value));
            return (0, ethers_1.toNumber)(value);
        }
        else {
            return value;
        }
    };
    return JSONbigAlways.stringify(value, xxx, space);
};
const args = new Map();
process.argv.slice(2).forEach(a => {
    const nameValue = a.split('=');
    args.set(nameValue[0], nameValue[1]);
});
if (args.size != 4) {
    throw new Error(`
incorrect usage, please provide:
 --rpcUrl=<rpcUrl>
 --txnHash=<txnHash>
 --traceConfig=<traceConfig> 
 --outputFilePath=<outputFilePath>`);
}
const rpcUrl = args.get('--rpcUrl');
if (!rpcUrl) {
    throw new Error('incorrect usage, please provide --<rpcUrl>');
}
const txnHash = args.get('--txnHash');
if (!txnHash) {
    throw new Error('incorrect usage, please provide --<txnHash>');
}
const traceConfig = args.get('--traceConfig');
if (!traceConfig) {
    throw new Error('incorrect usage, please provide --<traceConfig>');
}
const outputFilePath = args.get('--outputFilePath');
if (!outputFilePath) {
    throw new Error('incorrect usage, please provide --<outputFilePath>');
}
generateTest(rpcUrl, txnHash, traceConfig)
    .then(testCase => {
    const json = JSON.stringify(testCase, null, 2);
    fs_1.default.writeFile(outputFilePath, json, { flag: 'w+' }, (err) => {
        if (err != null) {
            console.error(err);
        }
    });
})
    .catch(console.error);
class TestCase {
    genesis;
    context;
    input;
    result;
    constructor(genesis, context, input, result) {
        this.genesis = genesis;
        this.context = context;
        this.input = input;
        this.result = result;
    }
}
async function generateTest(rpcUrl, txnHash, traceConfig) {
    const provider = new ethers_1.JsonRpcProvider(rpcUrl);
    const txnResponse = await provider.getTransaction(txnHash).catch(err => {
        throw err;
    });
    if (txnResponse == null) {
        throw new Error('null txnHash returned from provider');
    }
    const context = await generateContext(txnResponse);
    const genesis = await generateGenesis(provider, txnHash, context.number - 1);
    const result = await generateResult(provider, txnHash, JSON.parse(traceConfig));
    const input = ethers_1.Transaction.from(txnResponse).serialized;
    return Promise.resolve(new TestCase(genesis, context, input, result));
}
class Genesis {
    alloc;
    config;
    baseFeePerGas;
    constructor(alloc, config, baseFeePerGas) {
        this.alloc = alloc;
        this.config = config;
        this.baseFeePerGas = baseFeePerGas;
    }
}
async function generateGenesis(provider, txnHash, parentBlockNum) {
    const alloc = debugTraceTransaction(provider, txnHash, { 'tracer': 'prestateTracer' }).catch(err => {
        throw err;
    });
    // JSON.parse = function (text: string, reviver?: (this: any, key: string, value: any) => any): any {
    //     console.log("wtf")
    //     // return _parse(text, reviver)
    //     // return JSONbig.parse(text, reviver)
    //     return JSONbigAlways.parse(text, reviver)
    // }
    //
    // JSON.stringify = function (value: any, replacer?: ((this: any, key: string, value: any) => any) | (number | string)[] | null, space?: string | number): string {
    //     console.log("wtf stringify")
    //     // const res = _stringify(value, replacer, space)
    //     // if (!res) {
    //     //     return ""
    //     // }
    //     // return res
    //     // return JSONbig.stringify(value, null, space)
    //     const xxx = function bigintReplacer(key: any, value: any) {
    //         console.log("hihihihi")
    //         console.log(value)
    //         console.log(typeof value)
    //         if (typeof value === "bigint") {
    //             return value.toString();
    //         } else {
    //             return value;
    //         }
    //     }
    //     return JSONbigAlways.stringify(value, xxx, space)
    // }
    const nodeInfo = await provider.send('admin_nodeInfo', []).catch(err => {
        throw err;
    });
    // JSON.parse = _originalParse
    // JSON.stringify = _originalStringify
    const parentBlock = await provider.getBlock(parentBlockNum).catch(err => {
        throw err;
    });
    if (parentBlock == null) {
        throw new Error('null parentBlock returned from provider');
    }
    let baseFeePerGas;
    if (parentBlock.baseFeePerGas == null) {
        baseFeePerGas = null;
    }
    else {
        baseFeePerGas = parentBlock.baseFeePerGas.toString();
    }
    console.log("REMOVE");
    console.log(nodeInfo.protocols.eth.config.terminalTotalDifficulty);
    console.log(nodeInfo.protocols.eth.config.terminalTotalDifficulty.toString());
    return Promise.resolve(new Genesis(alloc, nodeInfo.protocols.eth.config, baseFeePerGas));
}
class Context {
    number;
    difficulty;
    timestamp;
    gasLimit;
    miner;
    constructor(number, difficulty, timestamp, gasLimit, miner) {
        this.number = number;
        this.difficulty = difficulty;
        this.timestamp = timestamp;
        this.gasLimit = gasLimit;
        this.miner = miner;
    }
}
async function generateContext(txnResponse) {
    const block = await txnResponse.getBlock().catch(err => {
        throw err;
    });
    if (block == null) {
        throw new Error('null block returned from provider');
    }
    return Promise.resolve(new Context(block.number, block.difficulty.toString(), block.timestamp, block.gasLimit.toString(), block.miner));
}
async function generateResult(provider, txnHash, traceConfig) {
    let res = await debugTraceTransaction(provider, txnHash, traceConfig).catch(err => {
        throw err;
    });
    delete res['time'];
    return Promise.resolve(res);
}
async function debugTraceTransaction(provider, txnHash, traceConfig) {
    return provider.send('debug_traceTransaction', [txnHash, traceConfig]);
}
