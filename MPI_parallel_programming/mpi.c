// Author: liu zhongkai

#include <math.h>
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

int mpi(int argc, char * argv[]) {
    int my_rank = 0;        // 节点编号
    int comm_sz = 0;        // 所有节点的个数
    int return_value = -1;  // 函数返回值

    return_value = MPI_Init(&argc, &argv);  // 初始化MPI
    if (return_value != 0) {
        printf("ERROR: MPI initialization failure,please check!\n");
        return -1;
    }

    return_value = MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);       // 获取当前节点号
    if (return_value != 0) {
        printf("ERROR: MPI has failed to get the current node number, please check!\n");
        return -1;
    }

    return_value = MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);       // 获取节点的总数
    if (return_value != 0) {
        printf("ERROR: MPI has failed to get the total number of nodes, please check\n");
        return -1;
    }

    if (comm_sz == 0) {
        printf("ERROR:The number of nodes can not be 0. Please reconfigure the number of nodes.\n");
        return -1;
    }
    int vector_num = atoi(argv[1]);                // 要计算的向量的个数
    if (vector_num == 0) {
        printf("ERROR:The number of vectors to be calculated can not be 0. Please reenter.\n");
        return -1;
    }
    int integer_num = vector_num / comm_sz;        // 总向量与节点数整除结果
    int node_process_num = 0;                      // 每个节点所需处理的向量个数
    if (my_rank == 0) {                            // 节点0还需处理无法整除情况下的多余向量
        node_process_num = integer_num + vector_num % comm_sz;
    } else {
        node_process_num = integer_num;
    }

    int *vector_array = (int *)malloc(sizeof(int) * vector_num);  // 分配大小为vector_num的int型内存空间，存储中间处理结果
    if (vector_array == NULL) {
        printf("ERROR：vector_array pointer allocation memory space failed, please check!\n");
        return -1;
    }
    int *node_vector_array = (int *)malloc(sizeof(int) * node_process_num);
    if (node_vector_array == NULL) {
        printf("ERROR: node_vector_array pointer allocation memory space failed, please check!\n");
        return -1;
    }
    int *recv_array = (int *)malloc(sizeof(int) * vector_num);    // 接受最终的向量结果
    if (recv_array == NULL) {
        printf("ERROR: recv_array pointer allocation memory space failed, please check!\n");
        return -1;
    }

    int start = my_rank * integer_num;         // 每个节点开始处理向量的位置
    int i = 0;
    for (i = start + 1; i <= (my_rank + 1) * integer_num; ++i) {       // 各节点处理其对应位置的向量
        node_vector_array[i - 1 - start] = i * i;
    }
    if (my_rank == 0) {    // 节点0需要处理额外多余的数据
        int j = integer_num;  // 指向节点0存储下标位置
        for (i = integer_num * comm_sz + 1; i <= vector_num; i++) {
            node_vector_array[j++] = i * i;
        }
    }
    int *recvcounts = (int *)malloc(comm_sz * sizeof(int));  // 记录从每个节点接受的数据个数
    if (recvcounts == NULL) {
        printf("ERROE: recvcounts pointer allocation memory space failed, please check!\n");
        return -1;
    }
    int *address_migration = (int *)malloc(comm_sz * sizeof(int));      // 相对于recv_array的偏移位置
    if (address_migration == NULL) {
        printf("ERROR: address_migration pointer allocation memory space failed, please check!\n");
        return -1;
    }
    recvcounts[0] = integer_num + vector_num % comm_sz;
    address_migration[0] = 0;
    for (i = 1; i < comm_sz; i++) {
        recvcounts[i] = node_process_num;
        address_migration[i] = recvcounts[0] + (i - 1) * integer_num;
    }
    return_value = MPI_Allgatherv(node_vector_array,
                                  node_process_num,
                                  MPI_INT,
                                  vector_array,
                                  recvcounts,
                                  address_migration,
                                  MPI_INT,
                                  MPI_COMM_WORLD);
    if (return_value != 0) {
        printf("ERROR: Failure of MPI_Allgather function!\n");
        return -1;
    }

    srand(my_rank);                      // 初始化随机数种子保证各节点的生成的随机数不同
    int rand_num = rand() % 1000 + 1;      // 生成随机数
    for (i = 0; i < vector_num; i++) {       // 向量和随机数相乘，产生新的向量
        vector_array[i] *= rand_num;
    }

    return_value = MPI_Allreduce(vector_array, recv_array, vector_num, MPI_INT, MPI_SUM, MPI_COMM_WORLD);   // 将各节点对应位置向量值相加
    if (return_value != 0) {
        printf("ERROR: Failure of MPI_Allreduce function!\n");
        return -1;
    }

    FILE *fp = fopen("./output/lzk.txt", "a+");      // 打开各节点lzk.txt文件，将产生的向量值存储到该文件中
    if (fp != NULL) {              // 打开文件是否出错
        for (i = 0; i < vector_num; ++i) {
            fprintf(fp, "%d ",recv_array[i]);
        }
        fclose(fp);   // 关闭文件指针
    } else {
        printf("ERROR:File open failed, file pointer fp is empty!\n");
        return -1;
    }

    return_value = MPI_Finalize();     // 结束MPI
    if (return_value != 0) {
        printf("ERROR: MPI failed to quit normal!\n");
        return -1;
    }
    free(address_migration);
    free(recvcounts);
    free(recv_array);
    free(node_vector_array);
    free(vector_array);
    return 0;
}

int main(int argc, char* argv[]) {
    int return_value = mpi(argc, argv);
    if (return_value != 0) {
        printf("ERROR: mpi function execution error!\n");
    }
    return 0;
}
