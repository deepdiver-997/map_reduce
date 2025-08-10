#include "../map_reduce.hpp"
#include <vector>
#include <iostream>

namespace std {
    template<>
    struct hash<std::pair<int, std::vector<double>>> {
        size_t operator()(const std::pair<int, std::vector<double>>& p) const {
            size_t seed = 0;
            seed ^= std::hash<int>{}(p.first) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            for (const auto& val : p.second) {
                seed ^= std::hash<double>{}(val) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            }
            return seed;
        }
    };
}

using namespace map_reduce;

class MatrixMapper : public Mapper<std::pair<int/*start row index*/, std::vector<double>/*row vectors*/>, std::pair<int/*start row index*/, std::vector<double>/*row vectors*/>> {
    public:
    MatrixMapper(const std::vector<std::vector<double>>& B) : B_trans(B), offset_(0) {}
    std::vector<std::pair<std::pair<int, std::vector<double>>, std::pair<int, std::vector<double>>>> map(const std::vector<std::pair<int, std::vector<double>>>& rows) override {
        if (!validate(rows[0].second.size(), rows)) {
            throw std::runtime_error("Matrix A's dimensions do not match for mapping");
        }
        if (!validate(B_trans[0].size(), B_trans)) {
            throw std::runtime_error("Matrix B's dimensions do not match for mapping");
        }
        if (rows[0].second.size() != B_trans.size()) {
            throw std::runtime_error("Matrix dimensions do not match for multiplication");
        }
        std::vector<std::vector<double>> tmp(B_trans.size(), std::vector<double>(rows.size(), 0.0));
        std::vector<std::pair<std::pair<int, std::vector<double>>, std::pair<int, std::vector<double>> > > result;
        if (B_trans.empty() || B_trans[0].size() != rows[0].second.size()) {
            throw std::runtime_error("Matrix dimensions do not match for mapping");
        }
        result.reserve(rows.size());
        for (size_t i = 0; i < rows.size(); ++i) {
            std::vector<double> tmp(rows[0].second.size(), 0.0);
            for (size_t j = 0; j < B_trans.size(); ++j) {
                tmp[j] = point_product(rows[i].second, B_trans[j]);
            }
            result.push_back({{rows[i].first, std::vector<double>()}, {rows[i].first, std::move(tmp)}});
        }
        return result;
    }

    bool validate(int cols, const std::vector<std::vector<double>>& matrix) {
        for (const auto& row : matrix) {
            if (row.size() != cols) {
                return false;
            }
        }
        return true;
    }
    bool validate(int cols, const std::vector<std::pair<int, std::vector<double>>>& matrix) {
        for (const auto& row : matrix) {
            if (row.second.size() != cols) {
                return false;
            }
        }
        return true;
    }
    private:
    std::vector<std::vector<double>> B_trans;
    int offset_;
    double point_product(const std::vector<double>& a, const std::vector<double>& b) {
        double sum = 0.0;
        for (size_t i = 0; i < a.size(); ++i) {
            sum += a[i] * b[i];
        }
        return sum;
    }
};
class MatrixReducer : public Reducer<std::pair<int, std::vector<double>>, std::pair<int, std::vector<double>>> {
public:
    std::pair<int, std::vector<double>> reduce(const std::pair<int, std::vector<double>>& key, const std::vector<std::pair<int, std::vector<double>>>& input) override {
        if (input.size() != 1)
            throw std::runtime_error("reduce input size != 1");
        return input[0];
    }
    void final_sort(std::vector<std::pair<std::pair<int, std::vector<double>>, std::pair<int, std::vector<double>>>>& out) {
        std::sort(out.begin(), out.end(), [](std::pair<std::pair<int, std::vector<double>>, 
            std::pair<int, std::vector<double>>> a, std::pair<std::pair<int, std::vector<double>>, 
            std::pair<int, std::vector<double>>> b){
            return a.second.first < b.second.first;
        });
    }
    static void show(const std::vector<std::pair<std::pair<int, std::vector<double>>, std::pair<int, std::vector<double>>>>& out) {
        for(const auto& row : out) {
            for(const auto& v: row.second.second)
                std::cout << v << " ";
            std::cout << std::endl;
        }
    }
};
class MatrixDivider : public Divider<std::pair<int, std::vector<double>>> {
public:
    MatrixDivider(std::vector<std::vector<double>>& A) : A_(A) {}
    std::vector<std::vector<std::pair<int, std::vector<double>>>> divide(const std::vector<std::pair<int, std::vector<double>>>& input, size_t chunks_size) override {
        std::vector<std::vector<std::pair<int, std::vector<double>>>> chunks;
        int average_rows = 1;//(A_.size() + chunks_size - 1) / chunks_size;
        chunks.reserve(A_.size() / average_rows + A_.size() % average_rows ? 1 : 0);
        for (size_t i = 0; i < A_.size();) {
            std::vector<std::pair<int, std::vector<double>>> chunk;
            for (size_t j = 0; j < average_rows && i < A_.size(); ++j, ++i) {
                chunk.emplace_back(i, A_[i]);
            }
            chunks.push_back(chunk);
        }
        return chunks;
    }
    virtual ~MatrixDivider() = default;
    private:
    std::vector<std::vector<double>> A_;
};

std::vector<std::vector<double>> transportMatrix(const std::vector<std::vector<double>>& input) {
    std::vector<std::vector<double>> output;
    if (input.empty()) return output;

    size_t rows = input.size();
    size_t cols = input[0].size();
    output.resize(cols, std::vector<double>(rows));

    for (size_t i = 0; i < rows; ++i) {
        for (size_t j = 0; j < cols; ++j) {
            output[j][i] = input[i][j];
        }
    }
    return output;
}

int main()
{
    std::vector<std::vector<double>> inputData_1 = {
        {1, 2, -1, 1},
        {2, -3, 1, -2},
        {4, 1, -1, 0}
    };
    std::vector<std::vector<double>> inputData_2 = {
        {0, 1, 2, 3},
        {4, 5, 6, 7},
        {8, 9, 10, 11},
        {12, 13, 14, 15}
    };
    auto transposedData = transportMatrix(inputData_2);
    Config config = {false, true, 4, 100};
    MapReduce<std::pair<int, std::vector<double>>, std::pair<int, std::vector<double>>> mapReduce(
        config,
        new MatrixMapper(transposedData),
        new MatrixReducer(),
        new MatrixDivider(inputData_1)
    );
    std::vector<std::pair<std::pair<int, std::vector<double>>, std::pair<int, std::vector<double>>>> output;
    mapReduce.run(std::vector<std::pair<int, std::vector<double>>>(), output);
    MatrixReducer::show(output);
    return 0;
}

// # 下载 Nginx 源码（以 1.24.0 为例）
// wget http://nginx.org/download/nginx-1.24.0.tar.gz
// tar -zxvf nginx-1.24.0.tar.gz
// cd nginx-1.24.0

// # 下载 ngx_http_proxy_connect_module
// git clone https://github.com/chobits/ngx_http_proxy_connect_module

// # 根据 Nginx 版本选择补丁（1.24.0 适用 1018 补丁）
// patch -p1 < ngx_http_proxy_connect_module/patch/proxy_connect_rewrite_1018.patch

// # 配置编译参数（保留原有参数，追加模块）
// ./configure \
//   --add-module=./ngx_http_proxy_connect_module \
//   --with-http_ssl_module        # 启用 SSL 支持

// # 编译并安装
// make
// sudo make install
// # 进入Nginx源码目录
// cd /root/Downloads/nginx-1.24.0

// # 撤销旧补丁（确保补丁文件存在）
// patch -p1 -R < ngx_http_proxy_connect_module/patch/proxy_connect_rewrite_XXXX.patch

// # 应用新补丁（替换XXXX为匹配版本号）
// patch -p1 < ngx_http_proxy_connect_module/patch/proxy_connect_rewrite_102101.patch

// server {
//   listen 8888;
//   resolver 8.8.8.8 ipv6=off;    # 关闭IPv6避免解析超时
//   proxy_connect;
//   proxy_connect_allow 443 80;   # 允许代理HTTPS(443)和HTTP(80)
//   proxy_connect_timeout 30s;
  
//   location / {
//     proxy_pass $scheme://$host$request_uri;
//     proxy_set_header Host $host;
//   }
// }
// # 启动Nginx
// sudo /usr/local/nginx/sbin/nginx

// # 测试代理功能
// curl -I https://www.baidu.com -v -x 127.0.0.1:8888
