//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_JOBMETRICS_H
#define TUPLEX_JOBMETRICS_H

#include <Base.h>
#include <unordered_map>
#include <ExceptionCodes.h>

namespace tuplex {
    // a collection of metrics, easily accessible
    // and updateable
    // When adding a new member variable, make sure to update to_json as well,
    // and the python metrics object!
    class JobMetrics {
    private:
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t> _exception_counts;
        std::unordered_map<std::tuple<int64_t, ExceptionCode>, ExceptionSample> _exceptions;
        double _logical_optimization_time_s = 0.0;
        double _llvm_optimization_time_s = 0.0;
        double _llvm_compilation_time_s = 0.0;
        double _total_compilation_time_s = 0.0;
        double _sampling_time_s = 0.0;

        // numbers per stage, can get combined in case.
        struct StageMetrics {
            int stageNo;
            double fast_path_wall_time_s = 0.0;
            double fast_path_time_s = 0.0;
            double slow_path_wall_time_s = 0.0;
            double slow_path_time_s = 0.0;
            double fast_path_per_row_time_ns = 0.0;
            double slow_path_per_row_time_ns = 0.0;
            // size_t fast_path_input_row_count;
            // size_t fast_path_output_row_count;
            // size_t slow_path_input_row_count;
            // size_t slow_path_output_row_count;
        };
        std::vector<StageMetrics> _stage_metrics;

        inline std::vector<StageMetrics>::iterator get_or_create_stage_metrics(int stageNo) {
            auto it = std::find_if(_stage_metrics.begin(), _stage_metrics.end(),
                                   [stageNo](const StageMetrics& m) { return m.stageNo == stageNo; });
            if(it == _stage_metrics.end()) {
                _stage_metrics.push_back(StageMetrics());
                _stage_metrics.back().stageNo = stageNo;
                it = std::find_if(_stage_metrics.begin(), _stage_metrics.end(),
                                  [stageNo](const StageMetrics& m) { return m.stageNo == stageNo; });
                assert(it != _stage_metrics.end());
            }
            return it;
        }

    public:
        size_t totalExceptionCount; //! how many exceptions occured in total for the job?

        inline void setExceptionCounts(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, size_t>& ecounts) {
            _exception_counts = ecounts;
        }

        inline void setExceptions(const std::unordered_map<std::tuple<int64_t, ExceptionCode>, ExceptionSample>& exceptions) {
            _exceptions = exceptions;
        }

        // set back to neutral values
        inline void reset() {
            totalExceptionCount = 0;
        }

        inline std::unordered_map<std::string, size_t> getOperatorExceptionCounts(int64_t operatorID) const {
            std::unordered_map<std::string, size_t> counts;
            for(const auto& keyval : _exception_counts) {
                auto opID = std::get<0>(keyval.first);
                auto ec = std::get<1>(keyval.first);
                auto c = keyval.second;
                if(opID == operatorID)
                    counts[exceptionCodeToPythonClass(ec)] = c;
            }
            return counts;
        }

        inline std::unordered_map<std::string, ExceptionSample> getOperatorExceptions(int64_t operatorID) const {
            std::unordered_map<std::string, ExceptionSample> exceptions;
            for(const auto& keyval : _exceptions) {
                auto opID = std::get<0>(keyval.first);
                auto ec = std::get<1>(keyval.first);
                auto c = keyval.second;
                if(opID == operatorID)
                    exceptions[exceptionCodeToPythonClass(ec)] = c;
            }
            return exceptions;
        }

        /*!
        * setter for logical optimization time
        * @param time a double representing logical optimization time in s
        */  
        void setLogicalOptimizationTime(double time) {
            _logical_optimization_time_s = time;
        }
        /*!
        * setter for llvm optimization time
        * @param time a double representing llvm optimization time in s
        */ 
        void setLLVMOptimizationTime(double time) {
            _llvm_optimization_time_s = time;
        }
        /*!
        * setter for compilation time via llvm
        * @param time a double representing compilation time via llvm in s
        */  
        void setLLVMCompilationTime(double time) {
            _llvm_compilation_time_s = time;
        }
        /*!
        * setter for total compilation time
        * @param time a double representing total compilation time in s
        */
        void setTotalCompilationTime(double time) {
            _total_compilation_time_s = time;
        }
        /*!
        * getter for logical optimization time
        * @returns a double representing logical optimization time in s
        */    
        double getLogicalOptimizationTime() {
            return _logical_optimization_time_s;
        }
        /*!
        * getter for llvm optimization time
        * @returns a double representing llvm optimization time in s
        */ 
        double getLLVMOptimizationTime() {
            return _llvm_optimization_time_s;
        }
        /*!
        * getter for compilation time via llvm
        * @returns a double representing compilation time via llvm in s
        */   
        double getLLVMCompilationTime() {
            return _llvm_compilation_time_s;
        }
        /*!
        * getter for total compilation time
        * @returns a double representing total compilation time in s
        */   
        double getTotalCompilationTime() {
            return _total_compilation_time_s;
        }

        /*!
         * set slow path timing info
         * @param stageNo
         * @param slow_path_wall_time_s
         * @param slow_path_time_s
         * @param slow_path_per_row_time_ns
         */
        void setSlowPathTimes(int stageNo,
                          double slow_path_wall_time_s,
                          double slow_path_time_s,
                          double slow_path_per_row_time_ns) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->slow_path_wall_time_s = slow_path_wall_time_s;
            it->slow_path_time_s = slow_path_time_s;
            it->slow_path_per_row_time_ns = slow_path_per_row_time_ns;
        }

        /*!
         * set fast path timing info
         * @param stageNo
         * @param fast_path_wall_time_s
         * @param fast_path_time_s
         * @param fast_path_per_row_time_ns
         */
        void setFastPathTimes(int stageNo,
                          double fast_path_wall_time_s,
                          double fast_path_time_s,
                          double fast_path_per_row_time_ns) {
            auto it = get_or_create_stage_metrics(stageNo);
            it->fast_path_wall_time_s = fast_path_wall_time_s;
            it->fast_path_time_s = fast_path_time_s;
            it->fast_path_per_row_time_ns = fast_path_per_row_time_ns;
        }

        /*!
         * set sampling time in s for specific operator
         * @param time
         */
        void setSamplingTime(double time) {
            _sampling_time_s = time;
        }

        /*!
         * create json representation of all datapoints
         * @return string in json format
         */
        std::string to_json() const {
            using namespace std;
            stringstream ss;
            ss<<"{";
            ss<<"\"logical_optimization_time_s\":"<<_logical_optimization_time_s<<",";
            ss<<"\"llvm_optimization_time_s\":"<<_llvm_optimization_time_s<<",";
            ss<<"\"llvm_compilation_time_s\":"<<_llvm_compilation_time_s<<",";
            ss<<"\"total_compilation_time_s\":"<<_total_compilation_time_s<<",";
            ss<<"\"sampling_time_s\":"<<_sampling_time_s<<",";

            // per stage numbers
            ss<<"\"stages\":[";
            for(const auto& s : _stage_metrics) {
                ss<<"{";
                ss<<"\"stage_no\":"<<s.stageNo<<",";
                ss<<"\"fast_path_wall_time_s\":"<<s.fast_path_wall_time_s<<",";
                ss<<"\"fast_path_time_s\":"<<s.fast_path_time_s<<",";
                ss<<"\"fast_path_per_row_time_ns\":"<<s.fast_path_per_row_time_ns<<",";
                ss<<"\"slow_path_wall_time_s\":"<<s.slow_path_wall_time_s<<",";
                ss<<"\"slow_path_time_s\":"<<s.slow_path_time_s<<",";
                ss<<"\"slow_path_per_row_time_ns\":"<<s.slow_path_per_row_time_ns;
                ss<<"}";
                if(s.stageNo != _stage_metrics.back().stageNo)
                    ss<<",";
            }
            ss<<"]";

            ss<<"}";
            return ss.str();
        }

    };
}
#endif //TUPLEX_JOBMETRICS_H