#include "../map_reduce.hpp"
using namespace map_reduce;


template <typename K = std::string, typename V = int>
class WordCountMapper : public Mapper<K, V> {
public:
    std::vector<std::pair<K, V>> map(const std::vector<K>& input) override {
        std::vector<std::pair<K, V>> result;
        auto func = [](char c){
            // if(c == ' ')return false;
            return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        };
        for (const auto& str : input) {
            std::string word = "";
            for(auto& c : str) {
                // if(c == 'g'){
                //     std::cout << "\nstart\n";
                // }
                if(func(c)) {
                    word += c;
                    continue;
                }
                if(!word.empty()) {
                    result.emplace_back(word, 1);
                    word = "";
                }
            }
            if(!word.empty()) {
                result.emplace_back(word, 1);
            }
        }
        // std::cout << "down\n";
        // for(auto& p : result) {
        //     std::cout << p.first << ": " << p.second << std::endl;
        // }
        return result;
    }
};

template <typename K = std::string, typename V = int>
class WordCountReducer : public Reducer<K, V> {
public:
    V reduce(const K& key, const std::vector<V>& input) override {
        return std::accumulate(input.begin(), input.end(), 0);
    }
};
class WordDivider: public Divider<std::string>
{
public:
    std::vector<std::vector<std::string>> divide(const std::vector<std::string>& input, size_t chunks_size) override {
        std::vector<std::vector<std::string>> chunks;
        // for (size_t i = 0; i < input.size(); i += chunks_size) {
        //     chunks.emplace_back(input.begin() + i, input.begin() + std::min(i + chunks_size, input.size()));
        // }
        chunks.emplace_back(input.begin(), input.end());
        return chunks;
    }
};


int main()
{
    // Example usage of MapReduce with a simple Mapper and Reducer
    Config config = {false, true, 4, 100};
    Mapper<std::string, int>* mapper = new WordCountMapper<>(); // Assume SimpleMapper is defined
    Reducer<std::string, int>* reducer = new WordCountReducer<>(); // Assume SimpleReducer is defined
    Divider<std::string>* divider = new WordDivider(); // Assume SimpleDivider is defined

    MapReduce<std::string, int> mapReduce(config, mapper, reducer, divider);

    std::vector<std::string> inputData = {"Hello world","world,hello","hi world","good bye"}; // Example input data
    std::vector<std::pair<std::string, int>> output;

    mapReduce.run(inputData, output);
    for(const auto& pair : output) {
        std::cout << pair.first << ": " << pair.second << std::endl;
    }

    return 0;
}