实现特征字符串、特征权重和特征ID的join
====
代码背景
----
    对于有监督的机器学习问题来说，训练完的模型存储的是特征和其对于的权重值。当数据量很大，有几万至更大数量特征的时候，特征首先需要ID化，将字符串类型的特征变为ID表示的特征，会得到一个字符串特征和ID的MAP文件：Feature_ID_Map。通过机器学习模型训练之后能得到一个特征ID和特征值的MAP文件:ID_Weight_Map。因此我们需要对这两个文件利用ID进行join操作，join之后存储结果：weight ID feature_str。<br>
    Feature_ID_Map文件：第一行是总特征数量，之后每一行格式：feature_str ID，以空格分割 。<br>
    ID_Weight_Map文件：每一行格式：weight ID, 以空格区分。<br>
    结果文件格式：weight ID feature_str。<br>
简单方案（不可行）
----
    对于几万甚至更多的特征进行join操作，如果利用单线程实现，并且简单的从一个文件一行开始，在另一个文件中根据ID进行查找，其时间复杂度是极其高的，同时会出现内存不足问题（文件较大）。<br>
实现方案
----
    因此我们采用mpi通信的多线程方式，快速处理该问题，同时时间复杂度极大的降低。<br>
    采用了内环换取时间复杂度的思想。<br>
    （1）每个节点读取Feature_ID_Map文件的每一行，开剖一个Map<int, string>m, 每个node只处理feature_id % node_num == my_node的特征ID。<br>
        存储方式是：m[feature_id / node_num] = feature_str。
    （2）每个节读取ID_Weight_Map每一行，每个节点只处理feature_id % node_num == my_node的行，通过m[feature_id / ndoe_num]找到feature_str。将weight ID feature_str写入文件。
    （3）最终主节点收集副节点的结果到一个文件中，完成上述的join操作。<br>
执行方式
----
    reverse_collextion.cpp 实现步骤（3）。<br>
    reverse_format.cpp 实现步骤（1），（2）。<br>
执行指令
----
    mpic++ reverse_format.cpp -o reverse_format <br>
    mpic++ reverse_collection.cpp -o reverse_collection <br>
    ./reverse_format ID_Weight_Map Feature_ID_Map result_file_name <br>
    (输出结果：result_file_name_0, result_file_name_1....result_file_name_10..)<br>
    ./reverse_collection output_file_name result_file_name <br>
    (output_file_name是主节点收集join之后的完整结果存储文件。result_file_name是所有节点通过步骤（1），（2）之后输出文件)<br>

