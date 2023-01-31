import numpy as np

print("Heramnb")
arrfromlist = np.array([3,4,5,5])
print("Array :",arrfromlist)
numpyarrayfromno1 =np.arange(1,20,3,dtype = np.float32)
print("The array from a arrange1" ,numpyarrayfromno1)
numpyarrayfromno2 =np.arange(1,20,3,dtype = np.int32)
print("The array from a arrange2" ,numpyarrayfromno2)

#Different ways to create the array
numpyarraycreate1=np.array([1,9,88,77])
print(f"the numpy array  created is as follows  {numpyarraycreate1} ")
numpyarraycreate2=np.arange(1,20,3,dtype='float32')
print(f"the numpy array  created is as follows  {numpyarraycreate2} ")

numpyarraycreate3=np.linspace(10,57,5,dtype='float32')
print(f"the numpy array  created is as follows  {numpyarraycreate3} ")

numpyarraycreate4_1=np.empty([4, 3], dtype = np.int32, order = 'f')
numpyarraycreate4_2=np.empty([4, 3], dtype = np.int32, order = 'c')

print(f"the numpy array  created is as follows-->numpyarraycreate4 \n {numpyarraycreate4_1} \n and \n {numpyarraycreate4_2} \n  ")

numpyarraycreate5_arraywithzeros=np.zeros([3, 4], dtype = np.int32,order = 'f')
print(f"the numpy array  created is as follows array with zeros is: \n  {numpyarraycreate5_arraywithzeros} ")