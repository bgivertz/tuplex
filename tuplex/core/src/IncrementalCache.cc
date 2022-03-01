//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/9/2022                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <IncrementalCache.h>

namespace tuplex {
    void IncrementalCache::clear() {
        // Call destructor on all entries and then clear
        for (auto keyval : _cache)
            delete keyval.second;
        _cache.clear();
    }

    CacheEntry::~CacheEntry() {
        // Invalidate partitions upon destruction
        for (auto p : _outputPartitions)
            p->invalidate();
        for (auto p : _exceptionPartitions)
            p->invalidate();
        for (auto p : _generalCasePartitions)
            p->invalidate();
    }

    void IncrementalCache::addCacheEntry(LogicalOperator *pipeline,
                                         const std::vector<Partition*> &outputPartitions,
                                         const std::vector<std::tuple<size_t, PyObject*>> &outputPythonObjects,
                                         const std::vector<Partition*> &exceptionPartitions,
                                         const std::unordered_map<std::string, ExceptionInfo> &exceptionsMap,
                                         const std::vector<Partition*> &generalCasePartitions,
                                         const std::unordered_map<std::string, ExceptionInfo> &generalCaseMap) {
        // Make partitions immortal between executions
        for (auto p : outputPartitions)
            p->makeImmortal();
        for (auto p : exceptionPartitions)
            p->makeImmortal();
        for (auto p : generalCasePartitions)
            p->makeImmortal();

        // Create and populate new cache entry
        auto cacheEntry = new CacheEntry();
        cacheEntry->setPipeline(pipeline->clone());
        cacheEntry->setOutputPartitions(outputPartitions);
        cacheEntry->setOutputPythonObjects(outputPythonObjects);
        cacheEntry->setExceptionPartitions(exceptionPartitions);
        cacheEntry->setExceptionsMap(exceptionsMap);
        cacheEntry->setGeneralCasePartitions(generalCasePartitions);

        // Temporarily cache only holds a single execution
        _cache["1"] = cacheEntry;
    }

    CacheEntry *IncrementalCache::getCacheEntry(LogicalOperator *action) const {
        // for now set all entries to same key, eventually change to compare logical plans
        auto elt = _cache.find("1");
        if (elt == _cache.end()) {
            return nullptr;
        }
        return elt->second;
    }
}
