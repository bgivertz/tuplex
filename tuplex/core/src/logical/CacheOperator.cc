//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <logical/CacheOperator.h>

namespace tuplex {


    // @TODO: need to save exception counts as well, so later stages can generate appropriate code!
    // => caching might also help, because if no exceptions are present no slow-code path needs to be executed/compiled!
    // => for join(.....cache()) case an upgrade must be compiled I fear...
    void CacheOperator::copyMembers(const LogicalOperator *other) {
        assert(other->type() == LogicalOperatorType::CACHE);

        LogicalOperator::copyMembers(other);
        auto cop = (CacheOperator*)other;
        setSchema(other->getOutputSchema());
        _normalCasePartitions = cop->cachedPartitions();
        _generalCasePartitions = cop->cachedExceptions();
        _partitionToExceptionsMap = cop->partitionToExceptionsMap();
        // copy python objects and incref for each!
        _py_objects = cop->_py_objects;
        python::lockGIL();
        for(auto obj : _py_objects)
            Py_XINCREF(obj);
        python::unlockGIL();
        _optimizedSchema = cop->_optimizedSchema;
        _cached = cop->_cached;
        _normalCaseRowCount = cop->_normalCaseRowCount;
        _generalCaseRowCount = cop->_generalCaseRowCount;
        _columns = cop->_columns;
        _sample = cop->_sample;
        _storeSpecialized = cop->_storeSpecialized;
    }

    LogicalOperator* CacheOperator::clone() {
        auto copy = new CacheOperator(parent()->clone(), _storeSpecialized, _memoryLayout);
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }

    CacheOperator * CacheOperator::cloneWithoutParents() const {
        auto copy = new CacheOperator(); // => no parents!
        copy->setDataSet(getDataSet());
        copy->copyMembers(this);
        assert(getID() == copy->getID());
        return copy;
    }

    int64_t CacheOperator::cost() const {
        // is operator cached? => return combined cost!
        // @NOTE: could make exceptions more expensive than normal rows
        if(isCached()) {
            return _generalCaseRowCount + _normalCaseRowCount;
        } else {
            // return parent cost
            return parent()->cost();
        }
    }

    void CacheOperator::setResult(const std::shared_ptr<ResultSet> &rs) {
        using namespace std;

        _cached = true;

        // fetch both partitions (consume) from resultset + any unresolved exceptions
        _normalCasePartitions = rs->partitions();
        for(auto p : _normalCasePartitions)
            p->makeImmortal();

        // @TODO: there are two sorts of exceptions here...
        // i.e. separate normal-case violations out from the rest
        // => these can be stored separately for faster processing!
        // @TODO: right now, everything just gets cached...

        _generalCasePartitions = rs->generalCasePartitions();
        for(auto p : _generalCasePartitions)
            p->makeImmortal();
        _partitionToExceptionsMap = rs->generalCaseMap();
        updateGeneralCase(rs->exceptionPartitions(), rs->exceptionMap());

        // check whether partitions have different schema than the currently set one
        // => i.e. they have been specialized.
        if(!_normalCasePartitions.empty()) {
            _optimizedSchema = _normalCasePartitions.front()->schema();
            assert(_optimizedSchema != Schema::UNKNOWN);
        }

        // if exceptions are empty, then force output schema to be the optimized one as well!
        if(_generalCasePartitions.empty())
            setSchema(_optimizedSchema);

        // because the schema might have changed due to the result, need to update the dataset!
        if(getDataSet())
            getDataSet()->setSchema(getOutputSchema());

        // print out some statistics about cached data
        size_t cachedPartitionsMemory = 0;
        size_t totalCachedPartitionsMemory = 0;
        size_t totalCachedRows = 0;
        size_t cachedExceptionsMemory = 0;
        size_t totalCachedExceptionsMemory = 0;
        size_t totalCachedExceptions = 0;

        int pos = 0;
        for(auto p : _normalCasePartitions) {
            totalCachedRows += p->getNumRows();
            cachedPartitionsMemory += p->bytesWritten();
            totalCachedPartitionsMemory += p->size();
            pos++;
        }
        for(auto p : _generalCasePartitions) {
            totalCachedExceptions += p->getNumRows();
            cachedExceptionsMemory += p->bytesWritten();
            totalCachedExceptionsMemory += p->size();
        }

        _normalCaseRowCount = totalCachedRows;
        _generalCaseRowCount = totalCachedExceptions;

        stringstream ss;
        ss<<"Cached "<<pluralize(totalCachedRows, "common row")
          <<" ("<<pluralize(totalCachedExceptions, "general row")
          <<"), memory usage: "<<sizeToMemString(cachedPartitionsMemory)
          <<"/"<<sizeToMemString(totalCachedPartitionsMemory)<<" ("
          <<sizeToMemString(cachedExceptionsMemory)
          <<"/"<<sizeToMemString(totalCachedExceptionsMemory)<<")";
        Logger::instance().defaultLogger().info(ss.str());

#ifndef NDEBUG
        // print schema
        Logger::instance().defaultLogger().info("CACHED common case schema: " + _optimizedSchema.getRowType().desc());
        Logger::instance().defaultLogger().info("CACHED general case schema: " + getOutputSchema().getRowType().desc());
#endif
    }

    size_t CacheOperator::getTotalCachedRows() const {
        size_t totalCachedRows = 0;
        for(auto p : _normalCasePartitions) {
            totalCachedRows += p->getNumRows();
        }
        for(auto p : _generalCasePartitions) {
            totalCachedRows += p->getNumRows();
        }
        return totalCachedRows;
    }

    void CacheOperator::updateGeneralCase(const std::vector<Partition*>& exceptionPartitions, const std::unordered_map<std::string, ExceptionInfo>& exceptionMap) {
        // if either are empty no updating necesary
        if (_generalCasePartitions.empty() || exceptionPartitions.empty()) {
            return;
        }

        // iterate over output partitions to update general case row inds
        for (const auto& partition : _normalCasePartitions) {
            auto generalInfo = _partitionToExceptionsMap.find(uuidToString(partition->uuid()))->second;
            auto generalInd = generalInfo.exceptionIndex;
            auto generalNumExps = generalInfo.numExceptions;
            auto generalNumRowsLeft = _generalCasePartitions[generalInd]->getNumRows() - generalInfo.exceptionRowOffset;
            auto generalPtr = _generalCasePartitions[generalInd]->lockWrite() + generalInfo.exceptionByteOffset;

            auto expInfo = exceptionMap.find(uuidToString(partition->uuid()))->second;
            auto expInd = expInfo.exceptionIndex;
            auto expNumExps = expInfo.numExceptions;
            auto expNumRowsLeft = exceptionPartitions[expInd]->getNumRows() - expInfo.exceptionRowOffset;
            auto expPtr = exceptionPartitions[expInd]->lock() + expInfo.exceptionByteOffset;

            auto numExpsProcessed = 0;
            auto numGeneralProcessed = 0;
            while (numExpsProcessed < expNumExps && numGeneralProcessed < generalNumExps) {
                auto generalRowInd = *((int64_t*)generalPtr);

                // Iterate over exceptions until row ind is larger than current general row ind
                while (numExpsProcessed < expNumExps && *((int64_t*) expPtr) < generalRowInd) {
                    expPtr += ((int64_t*)expPtr)[3] + sizeof(int64_t)*4;
                    numExpsProcessed += 1;
                    expNumRowsLeft -= 1;

                    // Change partitions if exhausted current one
                    if (expNumRowsLeft == 0) {
                        exceptionPartitions[expInd]->unlock();
                        expInd++;
                        if (expInd < exceptionPartitions.size()) {
                            expNumRowsLeft = exceptionPartitions[expInd]->getNumRows();
                            expPtr = exceptionPartitions[expInd]->lock();
                        }
                    }
                }

                // Decrement general row ind by number of exceptions that occured before it and iterate to next one
                *((int64_t*)generalPtr) -= numExpsProcessed;
                generalPtr += ((int64_t*)generalPtr)[3] + sizeof(int64_t)*4;
                numGeneralProcessed++;
                generalNumRowsLeft--;

                // Change partitions if exhausted current one
                if (generalNumRowsLeft == 0) {
                    _generalCasePartitions[generalInd]->unlockWrite();
                    generalInd++;
                    if (generalInd < _generalCasePartitions.size()) {
                        generalNumRowsLeft = _generalCasePartitions[generalInd]->getNumRows();
                        generalPtr = _generalCasePartitions[generalInd]->lockWrite();
                    }
                }
            }

            if (exceptionPartitions[expInd]->isLocked()) {
                exceptionPartitions[expInd]->unlock();
            }

            // Iterate over remaining general case partitions if any exist
            while (numGeneralProcessed < generalNumExps) {
                *((int64_t*)generalPtr) -= numExpsProcessed;
                generalPtr += ((int64_t*)generalPtr)[3] + sizeof(int64_t)*4;
                numGeneralProcessed++;
                generalNumRowsLeft--;

                // Change partitions if necessary
                if (generalNumRowsLeft == 0) {
                    _generalCasePartitions[generalInd]->unlockWrite();
                    generalInd++;
                    if (generalInd < _generalCasePartitions.size()) {
                        generalNumRowsLeft = _generalCasePartitions[generalInd]->getNumRows();
                        generalPtr = _generalCasePartitions[generalInd]->lockWrite();
                    }
                }
            }

            if (_generalCasePartitions[generalInd]->isLocked()) {
                _generalCasePartitions[generalInd]->unlockWrite();
            }
        }
    }
}