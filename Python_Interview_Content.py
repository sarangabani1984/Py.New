def mask_num(str1):
 import re
 alphanum=str1
 return re.sub(r'\d+', '@@@', alphanum)

print(mask_num("7411346811"))


strval=6
endval=10

# inputval = '7411346811'

user_input = input("Enter the masking number")