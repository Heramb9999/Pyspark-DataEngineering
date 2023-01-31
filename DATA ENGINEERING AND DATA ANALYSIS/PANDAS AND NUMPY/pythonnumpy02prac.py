import numpy as np
arrfromlist = np.array([3,4,5,5,88,89,43,27])

reshaped_array=arrfromlist.reshape((4,2),order='f')
print(f"the reshaped numpy ndarray is \n {reshaped_array}")


arrfromlist = np.array([[1,2,3],[4,5,6],[7,8,9]])
print(f" the array   is \n {arrfromlist}\n")
print(f" the full sum is {np.sum(arrfromlist)} for the array")
print(f" the  sum across x axis is {np.sum(arrfromlist, axis = 1)} ")
print(f" the  sum across y axis is {np.sum(arrfromlist, axis = 0)} ")

#how to flatten a nd array
arrfromlist1 = np.array([[1,2,3],[4,5,6],[7,8,9]])
flattenedarray=arrfromlist1.ravel()
print(f" the array   is \n {arrfromlist}\n")
print(f" the flattened array   is \n {flattenedarray}\n")