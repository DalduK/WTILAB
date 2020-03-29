from faker import Faker
import random
from random import randint
fake = Faker('pl_PL')
def fakeJson():
    for _ in range(10):
        my_dict = {'foo': randint(0, 100), 'bar': {'baz': fake.name(), 'poo': float(random.randrange(155, 389)) / 100}}
    return my_dict

#kod użytkownika min2bro do utworzenia szybkiego jsona z losowymi Imionami i Nazwiskami
#stackoverflow: https://stackoverflow.com/questions/55948258/how-to-generate-random-json-data-everytime-using-python