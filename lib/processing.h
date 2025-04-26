#pragma once

#include <optional>
#include <filesystem>
#include <iostream>
#include <vector>
#include <string>
#include <expected>
#include <fstream>
#include <stdexcept>
#include <unordered_map>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>
#include <cstdint>
#include <sstream>

#ifndef DEPARTMENT_DEFINED
#define DEPARTMENT_DEFINED
struct Department {
    std::string name;
    Department(const std::string& name) : name(name) {}
    bool operator==(const Department& other) const { return name == other.name; }
};
#endif


template <typename Key, typename Value>
struct KV {
    Key key;
    Value value;
};


template <typename Base, typename Joined>
struct JoinResult {
    Base base;
    std::optional<Joined> joined;

    bool operator==(const JoinResult& other) const {
        return (this->base == other.base && this->joined == other.joined);
    }
};


struct struct_AsVector {
    template <typename T>
    auto operator()(T&& flow) const {
        using val = typename std::decay_t<T>::value_type;
        std::vector<val> result;
        for (auto&& el : flow) {
            result.push_back(el);
        }
        return result;
    }
};

inline auto AsVector() {
    return struct_AsVector{};
}


namespace fs = std::filesystem;

struct Dir {
    using value_type = fs::path;

    fs::path dirName;
    std::vector<fs::path> files;

    explicit Dir(const std::string& pathStr) {
        dirName = fs::path(pathStr);
        if (fs::exists(dirName) && fs::is_directory(dirName)) {
            for (const auto& entry : fs::recursive_directory_iterator(dirName)) {
                if (entry.is_regular_file()) {
                    files.push_back(entry.path());
                }
            }
        }
    }

    Dir(const std::string& pathStr, bool recursive) {
        dirName = fs::path(pathStr);
        if (fs::exists(dirName) && fs::is_directory(dirName)) {
            if (recursive) {
                for (const auto& entry : fs::recursive_directory_iterator(dirName)) {
                    if (entry.is_regular_file()) {
                        files.push_back(entry.path());
                    }
                }
            } else {
                for (const auto& entry : fs::directory_iterator(dirName)) {
                    if (entry.is_regular_file()) {
                        files.push_back(entry.path());
                    }
                }
            }
        }
    }

    const std::vector<fs::path>& getFiles() const {
        return files;
    }

    auto begin()       { return files.begin(); }
    auto end()         { return files.end(); }
    auto begin() const { return files.begin(); }
    auto end()   const { return files.end(); }
};


struct OpenFiles {
    template <typename T>
    auto operator()(T&& flow) {
        std::vector<std::string> contents;
        for (auto&& file_path : flow) {
            std::ifstream file(file_path.string());
            if (!file.is_open()) {
                throw std::runtime_error("Cannot open file: " + file_path.string());
            }
            std::stringstream buffer;
            buffer << file.rdbuf();
            contents.push_back(buffer.str());
        }
        return contents;
    }
};


struct struct_Split {
    std::vector<std::string> delimiters;

    struct_Split(std::initializer_list<std::string> delims)
        : delimiters(delims)
    {}

    std::vector<std::string> operator()(const std::string& str) const {
        std::vector<std::string> result;
        size_t pos = 0, start = 0;

        while (pos < str.size()) {
            bool found_delim = false;
            size_t delim_len = 0;

            for (const auto& d : delimiters) {
                if (str.compare(pos, d.size(), d) == 0) {
                    found_delim = true;
                    delim_len = d.size();
                    break;
                }
            }

            if (found_delim) {
                result.push_back(str.substr(start, pos - start));
                pos += delim_len;
                start = pos;
            } else {
                pos++;
            }
        }
        if (start < str.size()) {
            result.push_back(str.substr(start));
        }
        return result;
    }
};

struct Split {
    std::vector<std::string> delimiters;

    Split(std::initializer_list<std::string> delims)
        : delimiters(delims)
    {}

    std::vector<std::string> operator()(const std::string& str) const {
        if (delimiters.empty()) {
            throw std::runtime_error("Нет заданных разделителей");
        }
        struct_Split s{ delimiters.front() };
        return s(str);
    }

};

struct SplitAdapter {
    explicit SplitAdapter(std::string delimiter)
        : m_delimiter(std::move(delimiter))
    {}

    template <typename Flow>
    auto operator()(Flow&& flow) const {
        std::vector<std::string> total;

        for (auto&& s : flow) {
            using ValueT = std::decay_t<decltype(s)>;

            if constexpr (std::is_same_v<ValueT, std::string>) {
                auto splitted = struct_Split({m_delimiter})(s);
                total.insert(total.end(), splitted.begin(), splitted.end());
            } else if constexpr (std::is_same_v<ValueT, std::stringstream>) {
                auto splitted = struct_Split({m_delimiter})(s.str());
                total.insert(total.end(), splitted.begin(), splitted.end());
            } else if constexpr (std::is_same_v<ValueT, std::ifstream>) {
                std::stringstream buffer;
                buffer << s.rdbuf();
                auto splitted = struct_Split({m_delimiter})(buffer.str());
                total.insert(total.end(), splitted.begin(), splitted.end());
            }
        }

        return total;
    }

    std::string m_delimiter;
};

inline auto Split(const std::string& delimiter) {
    return SplitAdapter(delimiter);
}

struct Out {
    std::ostream &os;

    explicit Out(std::ostream &out = std::cout) : os(out) {}
    template <typename Flow>
    auto operator()(const Flow & flow) const {
        for (auto & el : flow) {
            os << el << '\n';
        }
    }
};
template <typename Flow>
void operator|(const Flow & flow, const Out & out) {
    out(flow);
}

struct AsDataFlow_adapter {
    template<typename Container>
    Container& operator()(Container& container) {
        return container;
    }
};

template<typename Container>
Container& AsDataFlow(Container& container) {
    return AsDataFlow_adapter{}(container);
}

template <typename Func>
struct Transform {
    Func option;

    explicit Transform(Func f) : option(std::move(f)) {}

    template<typename Range>
    auto operator()(Range&& input) const {
        using input_type = typename std::decay_t<Range>::value_type;
        using result_type = decltype(option(std::declval<input_type>()));

        std::vector<result_type> result;
        auto b = input.begin();
        auto e = input.end();
        result.reserve(std::distance(b, e));

        for (auto it = b; it != e; ++it) {
            result.push_back(option(*it));
        }
        return result;
    }
};

template <typename Pred>
struct Filter {
    Pred option;

    explicit Filter(Pred f) : option(std::move(f)) {}

    template <typename Range>
    auto operator()(Range&& input) const {
        using value_type = typename std::decay_t<Range>::value_type;
        std::vector<value_type> result;

        auto b = input.begin();
        auto e = input.end();
        result.reserve(std::distance(b, e));

        for (auto it = b; it != e; ++it) {
            if (option(*it)) {
                result.push_back(*it);
            }
        }
        return result;
    }
};

struct Write {
    std::ostream& output;
    char delimiter;

    Write(std::ostream& file, char del)
        : output(file), delimiter(del)
    {}

    template <typename Range>
    auto operator()(Range&& r) const {
        for (auto&& el : r) {
            output << el << delimiter;
        }
        return std::forward<Range>(r);
    }
};

struct DropNullopt {
    template <typename Range>
    auto operator()(Range&& input) const {
        using optional_type = typename std::decay_t<Range>::value_type;
        using value_type = std::remove_reference_t<decltype(*std::declval<optional_type>())>;

        std::vector<value_type> result;
        for (auto&& opt : input) {
            if (opt.has_value()) {
                result.push_back(*opt);
            }
        }
        return result;
    }
};

template <typename RightRange, typename RightKeyFn, typename LeftKeyFn, typename CombineFn>
struct JoinImpl {
    RightRange right_range;
    RightKeyFn right_key;
    LeftKeyFn left_key;
    CombineFn comb;

    JoinImpl(RightRange rr, RightKeyFn rk, LeftKeyFn lk, CombineFn cf)
        : right_range(std::move(rr))
        , right_key(std::move(rk))
        , left_key(std::move(lk))
        , comb(std::move(cf))
    {}

    template <typename LeftRange>
    auto operator()(LeftRange&& lr) const {
        using L = typename std::decay_t<LeftRange>::value_type;
        using R = typename std::decay_t<RightRange>::value_type;
        using key_type = decltype(left_key(std::declval<L>()));
        using result_type = decltype(comb(std::declval<L>(), std::declval<std::optional<R>>()));

        std::unordered_map<key_type, R> right_map;
        for (const auto& r : right_range) {
            auto k = right_key(r);
            right_map[k] = r;
        }

        std::vector<result_type> out;
        out.reserve(std::distance(lr.begin(), lr.end()));

        for (const auto& leftEl : lr) {
            auto leftK = left_key(leftEl);
            std::optional<R> matched;
            if (auto it = right_map.find(leftK); it != right_map.end()) {
                matched = it->second;
            }
            out.push_back(comb(leftEl, matched));
        }
        return out;
    }
};

template <typename RightFlow>
struct JoinKVAdapter {
    RightFlow right_flow;

    explicit JoinKVAdapter(RightFlow rf)
        : right_flow(std::move(rf))
    {}

    template <typename LeftFlow>
    auto operator()(LeftFlow&& lf) const {
        using L = typename std::decay_t<LeftFlow>::value_type;
        using R = typename std::decay_t<RightFlow>::value_type;

        auto leftKeyFn = [](const L& x) { return x.key; };
        auto rightKeyFn = [](const R& x) { return x.key; };

        auto comb = [](const L& leftEl, std::optional<R> rightEl) {
            using LVal = decltype(leftEl.value);
            using RVal = decltype(std::declval<R>().value);
            if (!rightEl.has_value()) {
                return JoinResult<LVal, RVal>{ leftEl.value, std::nullopt };
            } else {
                return JoinResult<LVal, RVal>{ leftEl.value, rightEl->value };
            }
        };

        using JoinT = JoinImpl<RightFlow, decltype(rightKeyFn), decltype(leftKeyFn), decltype(comb)>;
        JoinT joiner(right_flow, rightKeyFn, leftKeyFn, comb);
        return joiner(std::forward<LeftFlow>(lf));
    }
};

template <typename RightFlow>
auto Join(RightFlow&& rf) {
    return JoinKVAdapter<std::decay_t<RightFlow>>(std::forward<RightFlow>(rf));
}


template <typename RightFlow, typename LeftKeyFn, typename RightKeyFn>
struct JoinKeyAdapter {
    RightFlow right_flow;
    LeftKeyFn lf;
    RightKeyFn rf;

    JoinKeyAdapter(RightFlow rflow, LeftKeyFn l, RightKeyFn rr)
        : right_flow(std::move(rflow)), lf(std::move(l)), rf(std::move(rr))
    {}

    template <typename LeftFlow>
    auto operator()(LeftFlow&& leftData) const {
        using L = typename std::decay_t<LeftFlow>::value_type;
        using R = typename std::decay_t<RightFlow>::value_type;

        auto comb = [](const L& lEl, std::optional<R> rEl) {
            return JoinResult<L, R>(lEl, rEl);
        };

        using JoinT = JoinImpl<RightFlow, RightKeyFn, LeftKeyFn, decltype(comb)>;
        JoinT impl(right_flow, rf, lf, comb);
        return impl(std::forward<LeftFlow>(leftData));
    }
};

template <typename RightFlow, typename LeftKeyFn, typename RightKeyFn>
auto Join(RightFlow&& rf, LeftKeyFn l, RightKeyFn r) {
    return JoinKeyAdapter<std::decay_t<RightFlow>, LeftKeyFn, RightKeyFn>(
        std::forward<RightFlow>(rf), std::move(l), std::move(r));
}



template <typename Range>
struct SplitExpectedOp {
    auto operator()(Range&& input) const {
        using Expected_type = typename std::decay_t<Range>::value_type;
        using Value_type = typename Expected_type::value_type;
        using Error_type = typename Expected_type::error_type;

        std::vector<Value_type> expected_result;
        std::vector<Error_type> error_result;

        for (auto&& el : input) {
            if (el.has_value()) {
                expected_result.push_back(*el);
            } else {
                error_result.push_back(el.error());
            }
        }
        return std::make_pair(error_result, expected_result);
    }
};

struct SplitExpected {
    template<typename Fn>
    explicit SplitExpected(Fn&&) {}

    SplitExpected() = default;

    template<typename Range>
    auto operator()(Range&& input) const {
        return SplitExpectedOp<Range>()(std::forward<Range>(input));
    }
};

template <typename Initial, typename Aggregator, typename KeyExtractor>
struct AggregateByKey {
    Initial initial;
    Aggregator aggregator;
    KeyExtractor key_extractor;

    AggregateByKey(Initial i, Aggregator a, KeyExtractor k)
        : initial(std::move(i)), aggregator(std::move(a)), key_extractor(std::move(k))
    {}

    template<typename Range>
    auto operator()(Range&& input) const {
        using Element_type = typename std::decay_t<Range>::value_type;
        using Key_type = decltype(key_extractor(std::declval<Element_type>()));
        using Agg_type = Initial;

        std::vector<std::pair<Key_type, Agg_type>> result;
        std::unordered_map<Key_type, std::size_t> key_to_index;

        auto begin = input.begin();
        auto end = input.end();
        result.reserve(std::distance(begin, end));

        for (auto it = begin; it != end; ++it) {
            auto key = key_extractor(*it);
            auto found = key_to_index.find(key);

            if (found == key_to_index.end()) {
                std::size_t idx = result.size();
                key_to_index[key] = idx;
                result.push_back({ key, initial });
                aggregator(*it, result.back().second);
            } else {
                aggregator(*it, result[found->second].second);
            }
        }
        return result;
    }
};

template <typename Flow, typename Adapter,
          typename = decltype(std::declval<Adapter>()(std::declval<Flow>()))>
auto operator|(Flow&& flow, Adapter&& adapter)
{
    return adapter(std::forward<Flow>(flow));
}
