// Data: 2018-11-1
// Function: join weight-featureid and featureid-origin according to featureid. 每个节点根据hash处理对应的join数据。
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <mpi.h>
#include <map>
#include <cstdlib>

/**
 * @Function: split weight-feature, output split result.
 * @author: liuzhongkai
 * @data: 2018-11-1
 * @param[in]:
 *          line: string, target to be splited
 * @param[out]:
 *          str: string type array of length 2, storage split result
 *               str[0]: weight
 *               str[1]: featureid
 * @Return:
 *          0: successful split
 *          1: line format is wrong
*/
int weight_feature_split(std::string str[], const std::string &line) {
    std::stringstream input(line);
    std::string result;
    int i = 0;
    while (input >> result) {
        if (i <= 1) {
            str[i] = result;
        }
        i++;
    }
    if (i != 2) {   //The result of the split is not two.
        return 1;
    }
    else {
        return 0;
    }
}

/**
 * @Function: split featureid-origin, output featureid in the split result.
 * @author: liuzhongkai
 * @data: 2018-11-1
 * @param[in]:
 *          line: string, target to be splited
 * @param[out]:
 *          featureid: string, featureid in the split result
 * @Return:
 *          0: successful split
 *          1: line format is wrong
*/
int feature_origin_split(std::string &featureid, const std::string &line) {
    std::stringstream input(line);
    std::string result;
    int i = 0;
    while (input >> result) {
        if (i == 0) {
            featureid = result;
        }
        i++;
    }
    if (i <= 1) {   //data error
        return 1;
    }
    else {
        return 0;
    }
}

/**
 * @Function: output feature_origin_map
 * @author: liuzhongkai
 * @data: 2018-11-1
 * @param[out]:
 *          m: map<int, std::string>, the result of feature_origin_map
 * @param[in]:
 *          filename2: string, the file save feature-origin.
 * @param[in]:
 *          comm_sz: int, total number of nodes
 * @param[in]:
 *          my_rank: int, current node number
 * @Return:
 *          0: feature_origin_map is successful
 *          1: filename2 data format is wrong
*/
int feature_origin_map(std::map<int, std::string> &m, const std::string &filename2, const int &comm_sz, const int &my_rank) {
    std::ifstream f2(filename2.c_str());
    if (!f2.is_open()) {
        std::cout << filename2 << " open is failure!" << std::endl;
        return 1;
    }
    std::string line;
    //The first line in filename2 is the number of features, no need to deal with.
    getline(f2, line);
    int count = 2;
    while (getline(f2, line)) {
        std::string featureid;
        int ret = feature_origin_split(featureid, line);
        //data format is wrong
        if (ret == 1) {
            f2.close();
            std::cout << filename2 << " data format is wrong in the " << count << " line: " << line << std::endl;
            return 1;
        }
        int index = atoi(featureid.c_str());
        //featureid = 0 is handled by the comm_sz node.
        if (index == 0 && my_rank == comm_sz) {
            m[0] = line;
        }
        else {
            //hash map
            if (index % comm_sz == my_rank) {
               m[index / comm_sz] = line;
            }
        }
        count++;
    }
    f2.close();
    return 0;
}

/**
 * @Function: output weight-featureid-origin in the filename3
 * @author: liuzhongkai
 * @data: 2018-11-1
 * @param[out]:
 *          filename3: string, the file save weight-featureid-origin
 * @param[in]:
 *          filename1: string, the file save feature-origin.
 * @param[in]:
 *          comm_sz: int, total number of nodes
 * @param[in]:
 *          my_rank: int, current node number
 * @param[in]:
 *          m: map<int, std::string>, the map save featureid-origin-map
 * @Return:
 *          0: weight-featureid-origin is successful
 *          1: filename1 data format is wrong
*/
int join(const std::string &filename1, const std::string &filename3, const int &comm_sz, const int &my_rank, std::map<int, std::string> &m) {
    std::ifstream f1(filename1.c_str());
    if (!f1.is_open()) {
        std::cout << filename1 << "open is failure!" << std::endl;
        return 1;
    }
    std::ofstream f3(filename3.c_str(), std::ios::trunc);
    if (!f3.is_open()) {
        f1.close();
        std::cout << filename3 << "open is failure!" << std::endl;
        return 1;
    }
    std::string line;
    int count = 1;
    while (getline(f1, line)) {
        std::string str[2];
        int ret = weight_feature_split(str, line);
        //data format is wrong
        if (ret == 1) {
            std::cout << filename1 + " data format is wrong in the " << count << " line: " << line << std::endl;
            f1.close();
            f3.close();
            return 1;
        }
        int featureid = atoi(str[1].c_str());
        //featureid = 0 is handled by the comm_sz node.
        if (featureid == 0 && my_rank == comm_sz) {
            f3 << str[0] + ' ' + m[0] + '\n';
        }
        else {
             if (featureid % comm_sz == my_rank) {
                f3 << str[0] + ' ' +  m[featureid / comm_sz] + '\n';
             }
        }
        count++;
    }
    f1.close();
    f3.close();
    return 0;
}

int main(int argv, char * argc[]) {
    if (argv != 4) {
        std::cout << "usage: ./trans filename1(weight-feature) filename2(featureidmap) filename3(file save result)" << std::endl;
        return 1;
    }
    std::string filename1(argc[1]);
    std::string filename2(argc[2]);
    std::string filename3(argc[3]);
    int my_rank = 0;
    int comm_sz = 0;
    MPI_Init(&argv, &argc);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    std::string temp;
    std::stringstream ss;
    ss << my_rank;
    ss >> temp;
    //The output file name is modified to: filename3_myrank
    filename3 = filename3 + "_" + temp;
    std::map<int ,std::string> m;
    int ret = feature_origin_map(m, filename2, comm_sz, my_rank);
    if (ret == 0) {
        join(filename1, filename3, comm_sz, my_rank, m);
    }
    MPI_Finalize();
    return 0;
}
