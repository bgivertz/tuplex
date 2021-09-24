//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_F64BATCH_H
#define TUPLEX_F64BATCH_H

namespace tuplex { namespace orc {

/*!
 * Implementation of OrcBatch for tuplex F64 types.
 */
class F64Batch : public OrcBatch {
public:

    F64Batch(::orc::ColumnVectorBatch *orcBatch, uint64_t numRows, bool isOption) : _orcBatch(
            static_cast<::orc::DoubleVectorBatch *>(orcBatch)) {
        _orcBatch->numElements = numRows;
        _orcBatch->hasNulls = isOption;
        _orcBatch->resize(numRows);
    }

    void setData(double value, bool notNull, uint64_t row) {
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * 2);
        }
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            _orcBatch->data[row] = value;
        }
    }

    void setData(tuplex::Deserializer &ds, uint64_t col, uint64_t row) override {
        auto notNull = !ds.isNull(col);
        auto value = ds.getDouble(col);
        setData(value, notNull, row);
    }

    void setData(tuplex::Field field, uint64_t row) override {
        auto notNull = !field.isNull();
        auto value = field.getDouble();
        setData(value, notNull, row);
    }

    void setBatch(::orc::ColumnVectorBatch *newBatch) override {
        _orcBatch = static_cast<::orc::DoubleVectorBatch *>(newBatch);
    }

    tuplex::Field getField(uint64_t row) override {
        using namespace tuplex;
        if (_orcBatch->hasNulls) {
            if (_orcBatch->notNull[row]) {
                return Field(option<double>((double) _orcBatch->data[row]));
            } else {
                return Field(option<double>::none);
            }
        } else {
            return Field((double) _orcBatch->data[row]);
        }
    }

private:
    ::orc::DoubleVectorBatch *_orcBatch;
};

}}

#endif //TUPLEX_F64BATCH_H
