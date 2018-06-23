### 多进程文件拷贝
### 任务:在某个路径下将1000多个文件拷贝到另一个文件夹下

from multiprocessing import Pool # 进程池
import hashlib  # 文件完整性检查
import os       # 获取某个路径下的文件信息
from multiprocessing import Manager


def innerCopyFile(fileName,srcPath,desPath,q):
    # 拼接路径名和文件名
    srcFileName = srcPath+"/"+fileName
    desFileName = desPath+"/"+fileName

    print(fileName)
    # 打开源文件,读入内容;写入到目标文件
    with open(srcFileName, 'rb') as fs:
        with open(desFileName, 'wb') as fw:
            for i in fs:
                fw.write(i)
    # 把拷贝完成的文件名放入队列,以主进程进行检查            
    q.put(fileName)
    return True

# 拷贝文件的操作
def CopyFile(fileName,srcPath,desPath,q):
    # 如果原路径不存在,则报错,返回
    if not os.path.exists(srcPath):
        print("srcPath is not exist")
        return None

    # 目标路径不存在,则创建出目标路径
    if not os.path.exists(desPath):
        try:
            os.mkdir(desPath)# 尝试创建目标路径        
        except NotImplementError:
            print("mkdir %s error"%desPath)
            return None

    # 真正的拷贝文件的操作    
    innerCopyFile(fileName,srcPath,desPath,q)
    return True

# 对文件做hash
def hashFile(fileName):
    hashM = hashlib.md5() #选择Hash算法
    with open(fileName,'rb') as f:
        for i in f:#读取文件内容做hash
            hashM.update(i)
    return hashM.hexdigest()#返回hash16进制表达


if __name__ == '__main__':
    # 输入原路径和目标路径
    srcPath = input("请输入你要拷贝的源文件路径:")
    #desPath = input("请输入你要拷贝的目标文件路径:")
    desPath = srcPath+"-副本"
    # 注意这里的while循环:只有在目标路径不存在的情况下
    #才会退出循环
    while os.path.isdir(desPath):
        desPath = desPath+"-副本"

    # 获取原路径下的文件列表
    allFiles = os.listdir(srcPath)
    # 获取原路径下的文件数量
    allNum = len(allFiles)
    print(allNum)

    # 通过进程池来拷贝文件
    p = Pool()
    q = Manager().Queue() # 在进程池中进行数据交互,使用Manger的队列
    for i in allFiles:
        # 用进程池异步加载任务的方式去调度拷贝任务
        p.apply_async(func=CopyFile, args=(i,srcPath,desPath,q))
    p.close()  #通知进程池不会再增加新的任务了

    # 主进程是监控的角色,去查看进程池中每一个任务完成是否合格
    num = 0
    while num < allNum:
        # 通过队列获取到新完成拷贝的一个目标文件
        fileName = q.get()

        # 用hash去检查文件是否拷贝完整
        srcFileName = srcPath+"/"+fileName
        desFileName = desPath+"/"+fileName
        if hashFile(srcFileName) == hashFile(desFileName):
            print("%s 拷贝成功"%desFileName)
        else:
            print("%s 拷贝失败"%desFileName)

        # 显示进度
        num += 1
        rate = num/allNum*100
        print("当前拷贝的进度是%.1f%%"%rate)


    p.join()   #等待进程池中的进程完成所有的任务
    print("Copy Done")

