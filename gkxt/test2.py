with open('/opt/bfd/aaa.csv','r') as f:
	data = f.readlines()
	for x in data:
	    a = x.replace('"','')
	    b = a.split(',')
	    if len(b)==2:
	        continue
	    wd = b[1]
	    jd = b[2]
	    try:
		if float(jd) < float(wd):
		    print jd,wd
	    except:
	        #print jd,wd
		pass
