package kvraft

// 定义一个全局变量 kvStorage
// var globalKVStorage = make(map[string]string)
// var globalRequestComplete = make(map[int64]bool)

type kvDatabase struct {
	KVStorage map[string]string //本地存储
	// KvrequestComplete map[int64]bool    //记录op是否完成
}

func newKVMachine() *kvDatabase {
	return &kvDatabase{
		KVStorage: make(map[string]string),
		// KvrequestComplete: make(map[int64]bool),
		// KvrequestComplete: globalRequestComplete,
	}
}
