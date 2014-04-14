import re

@outputSchema("bag:chararray") 
def ToCSV (bag):
	# convert list into string and remove {,},( and )
	bag = "".join(str(x) for x in bag)
	bag = bag.replace(',{(',' ')
	bag = re.sub(r'\(|\)|\{|\}','',bag)

	# get only the distinct hashtags
	bag = bag.split(',')
	bag = set(bag)
	bag = ''.join(str(x) for x in bag)
	return bag	

