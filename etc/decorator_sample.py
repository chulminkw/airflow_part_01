'''
- Python에서 decorator는 이미 만들어져 있는 함수를 소스 코드 변경 없이 특정 기능을 추가할 수 있게 해줌
- 로깅, 인증/권한부여, Framework의 특정 기능등을 수행하는 데 사용
- Airflow는 decorator를 활용하여 Taskflow API를 구현
- Python에서 function(함수)는 Object와 마찬가지로 1st class citizen(변수 할당, 함수인자, 함수 반환값 등에 사용될 수 있음)
- Python의 decorator는 decorator 내에서 수행될 함수를 인자로 받아서 이 함수의 실행 및 추가 기능을 수행하는 내포 함수를 반환
'''

# 함수의 인자로 수행할 함수를 입력하여 해당 함수를 기능 및 추가 기능을 실행하는 내포 함수를 반환
def my_decorator(func):
    def wrapper():
        print("before")
        func()
        print("after")
    return wrapper

def say_hello():
    print("hello")

print("##### 아래는 decorated_say_hello 수행 결과")
decorated_say_hello = my_decorator(say_hello)
print(type(decorated_say_hello))
decorated_say_hello()

# python decorator를 적용하기. @my_decorator로 decorated된 say_hello() 선언
@my_decorator
def say_hello():
    print("hello")

print("##### 아래는 decorator @my_decorator로 decorated된 say_hello() 함수 수행 결과")
say_hello()

# decorator 함수 선언
# 전달받은 함수를 감싸서 실행 결과를 비율로 변환
def ratio_decorator(decorated_fn):

    def wrapper(*args, **kwargs):
        result = decorated_fn(*args, **kwargs)
        return result / 100  # 결과를 비율로 변환

    return wrapper

# decorator 적용 대상 함수
@ratio_decorator
def add(num1, num2):
    """두 숫자를 더한 뒤 비율로 반환"""
    return num1 + num2


print("##### @ratio_decorator 적용 결과")
print(add(10, 15))