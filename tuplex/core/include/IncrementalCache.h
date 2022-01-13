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

namespace tuplex {

    class Partition;

    class IncrementalCache {
    private:
        // Valid outputs
        std::vector<Partition*> _lastPartitions;
        std::vector<std::tuple<size_t, PyObject *>> _lastPyObjects;

        // Exceptions to reprocess
        std::vector<Partition*> _lastExceptions; // correct row num with merge
        std::vector<Partition*> _lastGeneralCase; // incorrect row num with merge
    public:
        void setLastPartitions(const std::vector<Partition*> &partitions) {
            _lastPartitions = partitions;
        }

        void setLastExceptions(const std::vector<Partition*> &exceptions) {
            _lastExceptions = exceptions;
        }

        void setLastGeneralCase(const std::vector<Partition*> &generalCase) {
            _lastGeneralCase = generalCase;
        }

        void setLastPyObjects(const std::vector<std::tuple<size_t, PyObject *>> &pyObjects) {
            _lastPyObjects = pyObjects;
        }

        std::vector<Partition*> lastPartitions() { return _lastPartitions; }
        std::vector<Partition*> lastExceptions() { return _lastExceptions; }
        std::vector<std::tuple<size_t, PyObject *>> lastPyObjects() { return _lastPyObjects; }
        std::vector<Partition*> lastGeneralCase() { return _lastGeneralCase; }
    };
}

#endif //TUPLEX_INCREMENTALCACHE_H
