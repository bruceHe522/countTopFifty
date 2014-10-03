import sqlite3
import datetime
import operator
#some constant and data structures designed
monthInSeconds=2592000
appear = 0
class DomainCountTable:
    count=0
    domain=None
    def __str__(self):
        return '%s,%s'%(self.domain,self.count)
    
class DomainCountDateTable:
    count=0
    flag=0
    domain=None
    dateIntoTable=None
    def __str__(self):
        return '%s,%s,%s,%s'%(self.count,self.flag,self.domain,self.dateIntoTable)
    
class DomainGrowthTable:
    domain='random'
    rate=0
    totalcount=0
    lastMonthCount=0
    def __str__(self):
        return '%s,%s,%s,%s'%(self.domain,self.rate,self.totalcount,self.lastMonthCount)
    

def insertNewmail():
    #edit these 3 email addresses each time before this mock program runs,
    #each time the program runs this address act like new email into the mailing table
    str1 = 'brucelol@success1.com'
    str2='adb@success2.com'
    str3 = 'abcde@success3.com'
    
    #cur.execute('drop table if exists mailing;');cur.execute('create table mailing (addr VARCHAR(255) NOT NULL);')
    cur.execute('insert into mailing (addr) values(?)',(str1,) )
    cur.execute('insert into mailing (addr) values(?)',(str2,) )
    cur.execute('insert into mailing (addr) values(?)',(str3,) )
    conn.commit()
    return

def DomainCountDate_databaseupdate(dctlist):
    cur.execute('SELECT * FROM domainCountDate')
    data = cur.fetchall()
    #target table is empty, insert tuples directly
    #this program supposed to be run one time in each day. So each time the current date
    #should be the date program been executed
    if len(data) == 0:
        print('domainCountDate Table is empty, insert following tuples with current date:')
        for dct in dctlist:
            print(dct)
            innercur=conn.cursor()
            innercur.execute('INSERT INTO domainCountDate VALUES (?,?,?)',
                                (dct.domain,dct.count,datetime.date.today(),))
        conn.commit()
        print('domainCountDate Table contents:')
        cur1=conn.execute('select * from domainCountDate')
        for row in cur1:
            print(row[0]+' '+str(row[1])+' '+row[2])
        
    #target table is not empty
    else:
        print('domainCountDate Table is not empty.')
        dcdtlist = []
        cur1=conn.execute('SELECT dom,count,dateEntry FROM domainCountDate')
        for row in cur1:
            x=DomainCountDateTable()
            x.domain = row[0]
            x.count = row[1]
            x.dateIntoTable = datetime.datetime.strptime(row[2],'%Y-%m-%d')
            print(x)
            dcdtlist.append(x)
        conn.commit()
        #for each domain in dct, if count in dct is larger than dcdt.
        #update difference in count into dcdt if dct is larger.
        for dct in dctlist:
            domaincount = 0
            date = datetime.datetime.now()
            for j, dcdt in enumerate(dcdtlist):
                if dct.domain != dcdt.domain:
                    if j == len(dcdtlist)-1 and appear == 0:
                        innercur = conn.cursor()
                        innercur.execute('INSERT INTO domainCountDate VALUES (?,?,?)',
                                            (dct.domain,dct.count,datetime.date.today(),))
                        conn.commit()
                elif dct.domain == dcdt.domain:
                    domaincount = domaincount + dcdt.count
                    date = dcdt.dateIntoTable
                    appear = 1
            appear = 0
            if dct.count == domaincount:
                print('No additional email coming in this domain today!!!')
            elif (dct.count != domaincount) and ((datetime.datetime.now()-date).total_seconds() >= 85400):
                print('New member comming in today:')
                newcount = dct.count - domaincount
                print(newcount)
                innercur = conn.cursor()
                innercur.execute('INSERT INTO domainCountDate VALUES (?,?,?)', 
                                    (dct.domain,newcount,datetime.date.today(),))
                conn.commit()
                innercur1 = conn.cursor()
                innercur1.execute('select * from domainCountDate')
                for row in innercur1:
                    print('good')
                    print(row[0]+' '+str(row[1])+' '+row[2])
        
    return

def countTopFiftyDomains():
    dgtlist = []
    domainNm = 0
    cur.execute('select COUNT(DISTINCT dom) from domainCountDate')
    for row in cur:
        domainNm=row[0]
    dcdtlist = []
    cur1=conn.execute('SELECT dom,count,dateEntry FROM domainCountDate')
    for row in cur1:
        x=DomainCountDateTable()
        x.domain = row[0]
        x.count = row[1]
        x.dateIntoTable = datetime.datetime.strptime(row[2],'%Y-%m-%d')
        dcdtlist.append(x)
    '''This for loop basically act as a filter to get total emails and last month
        emails of each domain and store total email count and last month count into 
        table dgt along with the domain name.'''
    for n in range(domainNm):
        dgt=DomainGrowthTable()
        for dcdt in dcdtlist:
            if dcdt.flag == 0:
                if (dgt.domain != 'random') and (dgt.domain == dcdt.domain):
                    if (datetime.datetime.now()-dcdt.dateIntoTable).total_seconds() <= monthInSeconds:
                        dgt.totalcount = dgt.totalcount + dcdt.count
                        dgt.lastMonthCount = dgt.lastMonthCount + dcdt.count
                        dcdt.flag = 1
                    else:
                        dgt.totalcount = dgt.totalcount + dcdt.count
                        dcdt.flag = 1
                elif dgt.domain == 'random':
                    if (datetime.datetime.now()-dcdt.dateIntoTable).total_seconds() <= monthInSeconds:
                        dgt.domain = dcdt.domain
                        dgt.totalcount = dgt.totalcount + dcdt.count
                        dgt.lastMonthCount = dgt.lastMonthCount + dcdt.count
                        dcdt.flag = 1
                    else:
                        dgt.totalcount = dgt.totalcount + dcdt.count
                        dcdt.flag = 1
        dgt.rate = dgt.lastMonthCount/dgt.totalcount
        dgtlist.append(dgt)
    #if less than 50 domain then all of them are top 50
    print('***The top 50 domains in last 30 days***: ')
    if len(dgtlist) <= 50:
        for dgt in dgtlist:
            print(dgt.domain)
    elif len(dgtlist) > 50:
        #sort the dgtlist in Descending order and print the top 50
        dgtlist.sort(key=operator.attrgetter('rate'), reverse=True)
        for n, dgt in enumerate(dgtlist):
            if n<50:
                print(dgt.domain)
    return
    
def main():
    print('Connected to mail.db successfully')
    #run the function to insert some new email into the mailing table
    insertNewmail()
    cur.execute('select * from mailing')
    print('Original mailing table on daily updated basis:')
    for row in cur:
        print(row[0])
    #create a new view called domain to store the new statistics about mailing table
    cur.execute('DROP TABLE if EXISTS domain;')
    cur.execute('CREATE TABLE domain (dom text NOT NULL);')
    
    #cur.execute('DROP TABLE if EXISTS domainCountDate;')
    cur.execute('''CREATE TABLE IF NOT EXISTS domainCountDate 
    (dom text NOT NULL,count integer,dateEntry numeric,PRIMARY KEY (dom, dateEntry));''')
    #this steps fetches all the domains exists in mailing table and store into domain table
    cur.execute('select * from mailing')
    print('All Domains in the mailing table:')
    for row in cur:
        innercur=conn.cursor()
        innercur.execute('insert into domain (dom) values(?)',(row[0].split('@')[1],))
    conn.commit()
    
    DomainCountTablelist = []
    print('domainCountTableList is as follows:')
    cur.execute('SELECT dom,count(dom) FROM domain GROUP BY dom;')
    for row in cur:
        x=DomainCountTable()
        x.domain = row[0]
        x.count = row[1]
        print(x)
        DomainCountTablelist.append(x)
    
    DomainCountDate_databaseupdate(DomainCountTablelist)
    countTopFiftyDomains()
    return

if __name__ == '__main__':
    conn = sqlite3.connect('mail.db')
    cur=conn.cursor()
    main()
    conn.close()