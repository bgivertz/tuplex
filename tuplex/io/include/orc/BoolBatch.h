//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_BOOLBATCH_H
#define TUPLEX_BOOLBATCH_H

namespace tuplex { namespace orc {

/*!
 * Implementation of OrcBatch for tuplex BOOLEAN types.
 */
class BoolBatch : public OrcBatch {
public:

    BoolBatch(::orc::ColumnVectorBatch *orcBatch, uint64_t numRows, bool isOption) : _orcBatch(
            static_cast<::orc::LongVectorBatch *>(orcBatch)) {
        _orcBatch->numElements = numRows;
        _orcBatch->hasNulls = isOption;
    }

    void setData(tuplex::Field field, uint64_t row) override {
        if (row == _orcBatch->capacity) {
            _orcBatch->resize(_orcBatch->capacity * 2);
        }
        auto notNull = !field.isNull();
        _orcBatch->notNull[row] = notNull;
        if (notNull) {
            _orcBatch->data[row] = field.getInt();
        }
    }

    void setBatch(::orc::ColumnVectorBatch *newBatch) override {
        _orcBatch = static_cast<::orc::LongVectorBatch *>(newBatch);
    }

    tuplex::Field getField(uint64_t row) override {
        using namespace tuplex;
        if (_orcBatch->hasNulls) {
            if (_orcBatch->notNull[row]) {
                return Field(option<bool>((bool) _orcBatch->data[row]));
            } else {
                return Field(option<bool>::none);
            }
        } else {
            return Field((bool) _orcBatch->data[row]);
        }
    }

private:
    ::orc::LongVectorBatch *_orcBatch;
};

}}

#endif //TUPLEX_BOOLBATCH_H
