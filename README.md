/**
 * 分批Hash对比两个集合
 * 优势：100%准确，可处理任意规模数据
 * 注意：需要足够的磁盘暂存空间
 */

// 连接到MongoDB
const conn1 = db.getSiblingDB('source_db');
const conn2 = db.getSiblingDB('target_db');

// 配置参数
const BATCH_SIZE = 50000; // 每批处理文档数
const EXCLUDE_FIELDS = ['_id', 'updatedAt', 'createTime', 'syncTime']; // 排除字段
const SOURCE_COLL = 'source_collection';
const TARGET_COLL = 'target_collection';

// 1. 创建对比用的Hash索引集合
const tempHashColl = 'comparison_hash_temp';
db[tempHashColl].drop();

// 2. 计算源集合的Hash（分批处理）
let processed = 0;
let hasMore = true;

while (hasMore) {
    // 分批计算Hash并存储
    const sourceBatch = conn1[SOURCE_COLL]
        .find({}, { projection: excludeFields(EXCLUDE_FIELDS) })
        .skip(processed)
        .limit(BATCH_SIZE)
        .toArray();
    
    if (sourceBatch.length === 0) {
        hasMore = false;
        break;
    }
    
    const bulkOps = sourceBatch.map(doc => {
        const docHash = calculateHash(doc);
        return {
            insertOne: {
                document: {
                    _id: doc._id, // 保留_id用于关联
                    sourceHash: docHash,
                    targetHash: null
                }
            }
        };
    });
    
    db[tempHashColl].bulkWrite(bulkOps, { ordered: false });
    processed += sourceBatch.length;
    print(`已处理源集合: ${processed} 条`);
}

// 3. 计算目标集合的Hash并更新
processed = 0;
hasMore = true;

while (hasMore) {
    const targetBatch = conn2[TARGET_COLL]
        .find({}, { projection: excludeFields(EXCLUDE_FIELDS) })
        .skip(processed)
        .limit(BATCH_SIZE)
        .toArray();
    
    if (targetBatch.length === 0) {
        hasMore = false;
        break;
    }
    
    const bulkUpdates = targetBatch.map(doc => {
        const docHash = calculateHash(doc);
        return {
            updateOne: {
                filter: { _id: doc._id },
                update: { $set: { targetHash: docHash } },
                upsert: false
            }
        };
    });
    
    db[tempHashColl].bulkWrite(bulkUpdates, { ordered: false });
    processed += targetBatch.length;
    print(`已处理目标集合: ${processed} 条`);
}

// 4. 对比差异
print('\n========== 对比结果 ==========');

// 统计总数
const totalCount = db[tempHashColl].countDocuments();
print(`总文档数: ${totalCount}`);

// 完全相同的文档
const matchCount = db[tempHashColl].countDocuments({ 
    $expr: { $eq: ["$sourceHash", "$targetHash"] } 
});
print(`数据相同的文档数: ${matchCount}`);

// 源有但目标无
const onlyInSource = db[tempHashColl].countDocuments({ 
    targetHash: null 
});
print(`仅存在于源集合的文档数: ${onlyInSource}`);

// 目标有但源无（需要在目标集合中查找不在临时集合中的_id）
const targetIds = new Set(
    conn2[TARGET_COLL].distinct('_id', {})
        .map(id => id.toString())
);
const tempIds = new Set(
    db[tempHashColl].distinct('_id', {})
        .map(id => id.toString())
);
let onlyInTarget = 0;
for (let id of targetIds) {
    if (!tempIds.has(id)) {
        onlyInTarget++;
    }
}
print(`仅存在于目标集合的文档数: ${onlyInTarget}`);

// Hash值不同
const diffHash = db[tempHashColl].countDocuments({ 
    $and: [
        { sourceHash: { $ne: null } },
        { targetHash: { $ne: null } },
        { $expr: { $ne: ["$sourceHash", "$targetHash"] } }
    ]
});
print(`Hash值不同的文档数: ${diffHash}`);

// 5. 生成差异报告
if (diffHash > 0) {
    print('\n========== 差异详情 ==========');
    const diffs = db[tempHashColl].find({
        $and: [
            { sourceHash: { $ne: null } },
            { targetHash: { $ne: null } },
            { $expr: { $ne: ["$sourceHash", "$targetHash"] } }
        ]
    }).limit(10).toArray();
    
    diffs.forEach(diff => {
        print(`ID: ${diff._id}`);
        print(`  源Hash: ${diff.sourceHash}`);
        print(`  目标Hash: ${diff.targetHash}`);
    });
    
    if (diffHash > 10) {
        print(`... 还有 ${diffHash - 10} 个差异未显示`);
    }
}

// 辅助函数
function excludeFields(fields) {
    const projection = {};
    fields.forEach(field => {
        projection[field] = 0; // 排除字段
    });
    return projection;
}

function calculateHash(doc) {
    // 移除_id字段（如果需要）
    const docToHash = { ...doc };
    delete docToHash._id;
    
    // 对字段进行排序，确保相同内容生成相同Hash
    const sortedStr = JSON.stringify(docToHash, Object.keys(docToHash).sort());
    return hex_md5(sortedStr); // 需要引入MD5函数
}




// 使用聚合框架生成Hash对比
const result = db.source_collection.aggregate([
    {
        $project: {
            // 排除指定字段
            _id: 1,
            dataHash: {
                $function: {
                    body: function(doc) {
                        // 移除排除字段
                        const exclude = ['updatedAt', 'createTime'];
                        const cleanDoc = {};
                        Object.keys(doc).forEach(key => {
                            if (!exclude.includes(key) && key !== '_id') {
                                cleanDoc[key] = doc[key];
                            }
                        });
                        
                        // 生成字符串并Hash
                        const sorted = Object.keys(cleanDoc).sort()
                            .reduce((obj, key) => {
                                obj[key] = cleanDoc[key];
                                return obj;
                            }, {});
                        return JSON.stringify(sorted);
                    },
                    args: ["$$ROOT"],
                    lang: "js"
                }
            }
        }
    },
    { $out: "temp_source_hashes" }
], { allowDiskUse: true });
