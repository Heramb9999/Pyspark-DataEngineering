import numpy as np

'''
Example: (No Copy by Assigning)
View: This is also known as Shallow Copy. The view is just a view of the original array and view does not own the data.
 When we make changes to the view it affects the original array,
 and when changes are made to the original array it affects the view.

'''
# creating array
arr = np.array([2, 4, 6, 8, 10])

# assigning arr to nc
nc = arr
# both arr and nc have same id
print("id of arr", id(arr))
print("id of nc", id(nc))
# updating nc
nc[0]= 12
# printing the values
print("original array in example1- ", arr)
print("assigned array- ", nc)
'''
Example: (making a view and changing original array)
Copy: This is also known as Deep Copy. The copy is completely a new array and copy owns the data. 
When we make changes to the copy it does not affect the original array, and when changes are made to the original array it does not affect the copy.
'''

# creating array
arr1 = np.array([12, 14, 16, 18, 110])

# creating view
view1 = arr1.view()

# both arr and v have different id
print("id of arr", id(arr1))
print("id of v", id(view1))

# changing original array
# will effect view
arr1[0] = 192

# printing array and view
print("original array in view example2- ", arr1)
print("view- ", view1)


'''
 Example: (making a copy and changing original array)

Array Owning it’s Data:
To check whether array own it’s data in view and copy we can use the fact that every NumPy array has the attribute base that returns None 
if the array owns the data. Else, the base attribute refers to the original o

'''

# creating array
arr123 = np.array([2, 4, 6, 8, 10])

# creating copy of array
c122 = arr123.copy()

# creating view of array
view123 = arr.view()

print("printing base attribute of copy and view")
print(arr123.base)
print(c122.base)
print(view123.base)

arraynumpyexampledis = np.array([2, 4, 6, 8, 10])

print(arraynumpyexampledis)
