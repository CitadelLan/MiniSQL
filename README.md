# MiniSQL

### 目前问题
1. tableHeap里没有对unique属性做特判

### 弱影响问题
1. BPlusTree::GetValue对叶节点的RUnlatch暂时不使用（因为还不知道哪里加了latch，否则测试过不了）
2. BPlusTree中所有WUnlatch暂不使用
3. #1 #3 #4 自主测试仍未进行
4. #4 中对Unique/主键属性没有做特判？