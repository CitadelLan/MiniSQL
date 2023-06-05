# MiniSQL

### 目前问题
1. B+树只支持Unique类型，需要在插入的时候对重复元素做特殊处理（中间节点、叶节点）
2. #1 #3 #4 #5 自主测试仍未进行
3. #3 #4 #5 已有测试未进行
4. #5 未编写完毕
5. #2 中自主测试Valgrind与debug预想情况不一致（暂时搁置）

### 目前改动
1. BPlusTree::GetValue对叶节点的RUnlatch暂时不使用（因为还不知道哪里加了latch，否则测试过不了）
2. BPlusTree中所有WUnlatch暂不使用