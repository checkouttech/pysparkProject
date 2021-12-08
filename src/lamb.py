




# Python 3 code to people above 18 yrs
ages = [13, 90, 17, 59, 21, 60, 5]


adults = list(filter(lambda age: age>18, ages))

print (adults) 



print (filter ( lambda age: age>18, ages) )  


# filter ( funciton , list/sequence )



letters = ['a', 'e', 'i', 'o', 'u']


seq = ['g', 'e', 'e', 'j', 'k', 's', 'p', 'r']  

print ( list ( filter ( lambda i : i in letters , seq )) ) 


def  foo(x) :
    if x in letters: 
        return True 
    else :
        return False  

print ( list ( filter ( foo , seq )) ) 



li = [5, 7, 0, 22, 97, 54, 62, 77, 23, 73, 61]

print ( list ( map ( lambda x : x*2 , li ) ))  
print ( list ( filter ( lambda x : x*2 , li ) ))  

