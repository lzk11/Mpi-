// Data: 2018-11-2
// Function: Collect the results of the reverse_format.cpp function to the master node.
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <mpi.h>
#include <sstream>

/**
 * @Function: The master node receives data sent by other nodes and writes to file output_file
 * @author: liuzhongkai
 * @data: 2018-11-2
 * @param[out]:
 *          output_file: string, the file save collecting results
 * @param[in]:
 *          intput_file: string, the file store collected data
 * @param[in]:
 *          comm_sz: int, total number of nodes
 * @Return:
 *          0: succeed
 *          1: open file is failed
*/
int mpi_recv(const std::string &output_file, const int &comm_sz) {
    MPI_Status status;
    std::ofstream f1(output_file.c_str(), std::ios::app);
    if (!f1.is_open()) {
            std::cout << output_file << " open is failure!" << std::endl;
            return 1;
    }

    // 主节点循环等待其他节点数据的传输，先发送本次传输数据的长度len，之后开辟内存空间接收数据，接受到数据之后写入output_file
    while (1) {
        bool flag = false;  // 标识位：传输成功break退出
        for (int i = 1; i < comm_sz; i++) {  // 主节点从节点1到最后一个节点循环接收数据
            int len;
            MPI_Recv(&len, 1, MPI_INT, i, i * 10 + 1, MPI_COMM_WORLD, &status); // 接收本次传输数据的大小len， 如果len=-1，则是通知主节点传输结束。
            if (len == -1 && i == (comm_sz - 1)) {  // 接收到最后一个节点传输结束（数据量len=-1）的通知后标识为flag = true
                flag = true;
                break;
            }
            if (len == -1) {  //接收到除最后一个节点外的其他节点的结束传输通知
                continue;
            }
            char *buffer = new char[len];  // 开辟动态内存空间存储接受的数据
            MPI_Recv(buffer, len, MPI_CHAR, i, i * 10 + 2, MPI_COMM_WORLD, &status);  // 主节点接收数据，按照字节接收
            for (int i = 0; i < len; i++) {  // 按照字节写入output_file
                f1 << buffer[i];
                if (i == 10000) {
                    f1.flush();
                    i = 0;
                }
            }
            f1.flush();
        }
        if (flag) {   // flag=true主节点停止接受数据
            break;
        }
    }
    f1.close();
    return 0;
}
/**
 * @Function: Read input_file data and send it to the master node.
 * @author: liuzhongkai
 * @data: 2018-11-2
 * @param[in]:
 *          intput_file: string, the file store collected data
 * @param[in]:
 *          my_rank: int, the node number
 * @Return:
 *          0: succeed
 *          1: open file is failed
*/
int mpi_send(const int &my_rank, std::string &input_file) {
    std::ifstream f2(input_file.c_str());
    if (!f2.is_open()) {
        std::cout << input_file << " open is failure!" << std::endl;
        return 1;
    }
    std::string line;
    std::string result = "";
    int count = 0;
    while (getline(f2, line)) {
        result = result + line + "\n";  // getline读文件会把'\n'去掉（原因不详），为了写入output_file自动换行，此处添加了'\n'。
        count ++;
        if (count == 5000) {  // 每读取5000行就给主节点发送一次。
            int len = result.length();
            // First, send len (the length of the data) to inform the master node to open up the corresponding dynamic memory space for storing data, and then send the data. The tags sent twice are different. The tag sent twice is different
            MPI_Send(&len, 1, MPI_INT, 0, my_rank * 10 + 1, MPI_COMM_WORLD);
            MPI_Send(&result[0], len, MPI_CHAR, 0, my_rank * 10 + 2, MPI_COMM_WORLD);
            count = 0;
            result = "";
        }
    }
    // Send data that does not satisfy 5000 rows remaining
    int len = result.length();
    MPI_Send(&len, 1, MPI_INT, 0, my_rank * 10 + 1, MPI_COMM_WORLD);
    MPI_Send(&result[0], len, MPI_CHAR, 0, my_rank * 10 + 2, MPI_COMM_WORLD);
    // Inform the master node of the end of the transmission by len=-1
    len = -1;
    MPI_Send(&len, 1, MPI_INT, 0, my_rank * 10 + 1, MPI_COMM_WORLD);
    f2.close();
    return 0;
}

int main(int argv, char * argc[]) {
    if (argv != 3) {
       std::cout << "usage: ./reverse_collection output_file_name input_file_name" << std::endl;
       return 1;
    }
    int my_rank = 0;
    int comm_sz = 0;
    MPI_Init(&argv, &argc);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    MPI_Status status;

    std::string output_file(argc[1]);
    std::string input_file(argc[2]);
    std::string temp;
    std::stringstream ss;
    ss << my_rank;
    ss >> temp;
    input_file = input_file + "_" + temp;

    if (my_rank == 0) {
        std::ofstream f1(output_file.c_str(), std::ios::trunc);
        if (!f1.is_open()) {
            std::cout << output_file << " open is failure!" << std::endl;
            return 1;
        }
        std::ifstream f2(input_file.c_str());
        if (!f2.is_open()) {
            std::cout << input_file << " open is failure!" << std::endl;
            return 1;
        }
        std::string line;
        int count = 0;
        // Node 0 reads input_file directly and then writes to output_file
        while (getline(f2, line)) {
            f1 << line << std::endl;
        }
        f1.close();
        f2.close();
        // Accept input from other nodes and write to output_file
        int ret = mpi_recv(input_file, output_file, comm_sz);
    }
    else {
        int ret = mpi_send(my_rank, input_file);
    }
    MPI_Finalize();
    return 0;
}