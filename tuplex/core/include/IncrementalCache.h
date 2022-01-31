//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/9/2022                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_INCREMENTALCACHE_H
#define TUPLEX_INCREMENTALCACHE_H

#include <Partition.h>
#include <logical/LogicalOperator.h>
#include <ExceptionInfo.h>

namespace tuplex {

    class Partition;
    class LogicalOperator;

    class CacheEntry {
    private:
        LogicalOperator *_pipeline; //! unoptimized logical pipeline of the cache entry
        std::vector<Partition*> _outputPartitions; //! output partitions from the previous execution
        std::vector<std::tuple<size_t, PyObject*>> _outputPythonObjects; //! output python objects from the previous execution
        std::vector<Partition*> _exceptionPartitions; //! exceptions raised during the previous execution
        std::unordered_map<std::string, ExceptionInfo> _exceptionsMap; //! mapping of exceptions to output partitions
        std::vector<Partition*> _generalCasePartitions; //! general case exceptions raised during the previous execution
        std::unordered_map<std::string, ExceptionInfo> _generalCaseMap; //! mapping of general case exceptions to output partitions
    public:
        CacheEntry() {};
        ~CacheEntry();

        void setPipeline(LogicalOperator *pipeline) { _pipeline = pipeline; }
        void setOutputPartitions(const std::vector<Partition*> &outputPartitions) { _outputPartitions = outputPartitions; }
        void setOutputPythonObjects(const std::vector<std::tuple<size_t, PyObject*>> &outputPythonObjects ) { _outputPythonObjects = outputPythonObjects; }
        void setExceptionPartitions(const std::vector<Partition*> &exceptionPartitions) { _exceptionPartitions = exceptionPartitions; }
        void setExceptionsMap(const std::unordered_map<std::string, ExceptionInfo> &exceptionsMap) { _exceptionsMap = exceptionsMap; }
        void setGeneralCasePartitions(const std::vector<Partition*> &generalCasePartitions) { _generalCasePartitions = generalCasePartitions; }
        void setGeneralCaseMap(const std::unordered_map<std::string, ExceptionInfo> &generalCaseMap) { _exceptionsMap = generalCaseMap; }

        LogicalOperator *pipeline() const { return _pipeline; }
        std::vector<Partition*> outputPartitions() const { return _outputPartitions; }
        std::vector<std::tuple<size_t, PyObject*>> outputPythonObjects() const { return _outputPythonObjects; }
        std::vector<Partition*> exceptionPartitions() const { return _exceptionPartitions; }
        std::unordered_map<std::string, ExceptionInfo> exceptionsMap() const { return _exceptionsMap; }
        std::vector<Partition*> generalCasePartitions() const { return _generalCasePartitions; }
        std::unordered_map<std::string, ExceptionInfo> generalCaseMap() const { return _generalCaseMap; }
    };

    class IncrementalCache {
    private:
        std::unordered_map<std::string, CacheEntry*> _cache;

    public:
        void clear();

        /*!
         * add entry to the incremental cache
         * @param pipeline unoptimized logical pipeline
         * @param outputPartitions output partitions of normal rows
         * @param outputPythonObjects output python objects
         * @param exceptionPartitions exceptions raised during execution
         * @param exceptionsMap mapping of exceptions to output partitions
         * @param generalCasePartitions general case exceptions raised during execution
         * @param generalCaseMap mapping of general case exceptions to output partitions
         */
        void addCacheEntry(LogicalOperator *pipeline,
                           const std::vector<Partition*> &outputPartitions,
                           const std::vector<std::tuple<size_t, PyObject*>> &outputPythonObjects,
                           const std::vector<Partition*> &exceptionPartitions,
                           const std::unordered_map<std::string, ExceptionInfo> &exceptionsMap,
                           const std::vector<Partition*> &generalCasePartitions,
                           const std::unordered_map<std::string, ExceptionInfo> &generalCaseMap);

        CacheEntry *getCacheEntry(LogicalOperator *action) const;
    };
}

#endif //TUPLEX_INCREMENTALCACHE_H