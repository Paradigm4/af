/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2016 SciDB, Inc.
* All Rights Reserved.
*
* accelerated_io_tools is a plugin for SciDB, an Open Source Array DBMS maintained
* by Paradigm4. See http://www.paradigm4.com/
*
* accelerated_io_tools is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* accelerated_io_tools is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with accelerated_io_tools.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#include <limits>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <ctype.h>
#include <algorithm>

#include <system/Exceptions.h>
#include <system/SystemCatalog.h>
#include <system/Sysinfo.h>

#include <query/TypeSystem.h>
#include <query/FunctionDescription.h>
#include <query/FunctionLibrary.h>
#include <query/PhysicalOperator.h>
#include <query/TypeSystem.h>
#include <query/FunctionLibrary.h>
#include <array/Tile.h>
#include <array/TileIteratorAdaptors.h>
#include <util/Platform.h>
#include <network/Network.h>
#include <array/SinglePassArray.h>
#include <array/SynchableArray.h>
#include <array/PinBuffer.h>

#include <boost/algorithm/string.hpp>
#include <boost/unordered_map.hpp>


namespace scidb
{

static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.af"));

using namespace scidb;
using std::shared_ptr;
using std::vector;

static void EXCEPTION_ASSERT(bool cond)
{
    if (! cond)
    {
        throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "Internal inconsistency";
    }
}

//Modeled after MaterializeArray
class AfArray : public DelegateArray
{
public:
    std::vector<int64_t> _coords;
    class ArrayIterator : public DelegateArrayIterator
    {
        AfArray& _array;

    public:
        void operator ++() override;
        bool setPosition(Coordinates const& pos) override;
        void restart() override;
        ArrayIterator(AfArray& arr, AttributeDesc& attrID, std::shared_ptr<ConstArrayIterator> input);

    private:
        bool coordinatePresent(int64_t coord);
    };

    AfArray(std::shared_ptr<Array> input, std::shared_ptr<Query>const& query, std::vector<int64_t> const& coords);
    virtual DelegateArrayIterator* createArrayIterator(AttributeID id) const;
};

bool AfArray::ArrayIterator::coordinatePresent(int64_t coord)
{

    return std::binary_search(_array._coords.begin(), _array._coords.end(), coord);
}

AfArray::ArrayIterator::ArrayIterator(AfArray& arr, AttributeDesc& attrID, std::shared_ptr<ConstArrayIterator> input):
    DelegateArrayIterator(arr, attrID, input),
    _array(arr)
{
    restart();
}

void AfArray::ArrayIterator::operator ++()
{
    DelegateArrayIterator::operator ++();
    while(! DelegateArrayIterator::end())
    {
        if( coordinatePresent(DelegateArrayIterator::getPosition()[0]))
        {
            return;
        }
        DelegateArrayIterator::operator ++();
    }
}

bool AfArray::ArrayIterator::setPosition(Coordinates const& pos)
{
    bool set = DelegateArrayIterator::setPosition(pos);
    if(!set)
    {
        return false;
    }
    if(coordinatePresent(DelegateArrayIterator::getPosition()[0] ))
    {
        return true;
    }
    return false;
}

void AfArray::ArrayIterator::restart()
{
    DelegateArrayIterator::restart();
    if(!DelegateArrayIterator::end() && !coordinatePresent(DelegateArrayIterator::getPosition()[0]))
    {
        operator ++();
    }
}

AfArray::AfArray(std::shared_ptr<Array> input,
                 std::shared_ptr<Query>const& query,
                 std::vector<int64_t> const& coords):
    DelegateArray(input->getArrayDesc(), input, true)
{
    _query = query;
    _coords = coords;
}

DelegateArrayIterator* AfArray::createArrayIterator(AttributeID id) const
{
    AttributeDesc attrDesc = inputArray->getArrayDesc().getAttributes().findattr(id);
    return new AfArray::ArrayIterator(*(AfArray*)this,
                                      attrDesc,
                                      inputArray->getConstIterator(attrDesc));
}

class PhysicalAf : public PhysicalOperator
{
public:
    PhysicalAf(std::string const& logicalName,
               std::string const& physicalName,
               Parameters const& parameters,
               ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    std::shared_ptr< Array> execute(std::vector< std::shared_ptr< Array> >& inputArrays, std::shared_ptr<Query> query)
    {
        shared_ptr<Array>& input = inputArrays[0];
        std::vector<int64_t> coords;
        shared_ptr<Array> idx = inputArrays[1];
        idx = redistributeToRandomAccess(idx, createDistribution(dtReplication), ArrayResPtr(), query, shared_from_this());
        shared_ptr<ConstArrayIterator> aiter = idx->getConstIterator(input->getArrayDesc().getAttributes().firstDataAttribute());
        while(! aiter->end())
        {
            shared_ptr<ConstChunkIterator> citer = aiter->getChunk().getConstIterator();
            while( ! citer->end())
            {
                Value const& v = citer->getItem();
                if (! v.isNull())
                {
                  coords.push_back( v.getInt64() );
                }
                ++(*citer);
            }
            ++(*aiter);
        }
        std::sort(coords.begin(), coords.end());
        return shared_ptr<Array>(new AfArray(input, query, coords));
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhysicalAf, "af", "PhysicalAf");

} // end namespace scidb
