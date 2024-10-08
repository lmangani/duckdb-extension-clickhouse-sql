#include <duckdb.hpp>
#include <hash_map>
#include <map>
#include <unordered_map>

namespace duckdb {
    struct SillyTreeKey {
        //TODO: key-value pair using duckdb::Value
    };
    struct SillyTreeValue {
        //TODO: key-value pair using duckdb::Value
    };
    static std::unordered_map<idx_t, std::map<SillyTreeKey, SillyTreeValue>> silly_btree_store;
    static std::unordered_map<idx_t, unique_ptr<std::mutex>> silly_btree_store_mutex;
    static std::mutex silly_btree_store_mutex_lock;



    struct  _lock {
        std::mutex &_m;
        _lock(std::mutex &m) : _m(m) {
            _m.lock();
        }
        ~_lock() {
            _m.unlock();
        }
    };

    struct AppendTreeData:FunctionData {
        unique_ptr<FunctionData> Copy() const override {
            throw std::runtime_error("not implemented");
        }

        bool Equals(const FunctionData &other) const override {
            return false;
        }

        idx_t tree_id;
        Value key;
        Value value;
    };

    static unique_ptr<AppendTreeData> AppendTreeBind(ClientContext &context, TableFunctionBindInput &input,
        vector<LogicalType> &return_types, vector<string> &names) {
        auto res = make_uniq<AppendTreeData>();
        res->tree_id = input.inputs[0].GetValue<idx_t>();
        res->key = input.inputs[1].GetValue<Value>();
        res->value = input.inputs[2].GetValue<Value>();
        return_types.clear();
        return_types.push_back(LogicalType::TINYINT);
        names.clear();
        names.push_back("ok");
        return res;
    }

    static unique_ptr<LocalTableFunctionState> AppendTreeLocalState(
        ClientContext &context, TableFunctionBindInput &input,
        vector<LogicalType> &return_types, vector<string> &names) {
        return nullptr;
    }

    struct SillyTree {
        std::map<Value, Value> &values;
        std::unique_ptr<_lock> lock;
        SillyTree(std::map<Value, Value> &values, std::unique_ptr<_lock> lock): values(values), lock(std::move(lock)) {}
    };

    static unique_ptr<SillyTree> GetTree(idx_t tree_id) {
        auto l = _lock(silly_btree_store_mutex_lock);
        auto it = silly_btree_store.find(tree_id);
        if (it == silly_btree_store.end()) {
            silly_btree_store[tree_id] = std::map<SillyTreeKey, SillyTreeValue>();
            silly_btree_store_mutex[tree_id] = make_uniq<std::mutex>();
        }

    }

    static void AppendTreeImplementation(ClientContext &context, duckdb::TableFunctionInput &data_p,DataChunk &output) {

    }

    static void RegisterSillyBtreeStore(DatabaseHeader &instance) {
        //TODO: register store functions
    }
}