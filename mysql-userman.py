#-*- coding: utf-8 -*-

# Project   : Grant or Revoke to MySQL users.
# Author    : YW. Jang
# Date      : 2016.06.07

import subprocess
import optparse
import sys # exit

class Instructor():
  def __init__(self):
    self._db_ca = {
      "web": ["web@'10.2.%'"],
      "was": ["web@'10.2.%'", "was@'10.1.%'"]
      }

    self._insensitive_ca_key = dict()
    for i in self._db_ca.keys():
      self._insensitive_ca_key[i.lower()] = i

    self._db_og = {
      "benjamin": ["benjamin@'10.0.%'"],
      "test": ["test@'%'"]
      }

    self._insensitive_og_key = dict()
    for i in self._db_og.keys():
      self._insensitive_og_key[i.lower()] = i
      
  def checkValidation(self, user, region):
    if region == "us-west-1":
      return user in self._insensitive_ca_key
    elif region == "us-west-2":
      return user in self._insensitive_og_key
    else:
      return False

  def chooseuser(self, region, user):
    region = region.lower()
    user = user.lower()

    if region == "us-west-1":
      return self._db_ca[self._insensitive_ca_key[user]]
    elif region == "us-west-2":
      return self._db_og[self._insensitive_og_key[user]]
    else:
      return ""

  def getOperation(self, user, privilege):
    query = privilege
    
    if user == "web":
      query += "SELECT "
    elif user == "was":
      query += "SELECT, INSERT, UPDATE, DELETE "
    else:
      query += "SELECT "
    
    return query

  def grant(self, tb, user, db, region):
    self.makePrivilegeQuery(tb, user, db, region)

    for q in self._query_list:
      exe_cmd = "mysql --login-path=admin -e\"%s\"" % q
      print("Excuted : " + q )
      subprocess.call(exe_cmd, shell=True)


class Granter(Instructor):
  def __init__(self):
    Instructor.__init__(self)
    self._query_list = list()

  def makePrivilegeQuery(self, tb, user, db, region):
    for user_host in self.chooseuser(region, user):
      query = ""
      user = (user_host.split("@"))[0]
      query = self.getOperation(user, "GRANT ")
      query += ("ON %s.%s TO %s;" % (db, tb, user_host))
      self._query_list.append(query)
        

class Revoker(Instructor):
  def __init__(self):
    Instructor.__init__(self)
    self._query_list = list()

  def makePrivilegeQuery(self, tb, user, db, region):
    for user_host in self.chooseuser(region, user):
      query = ""
      user = (user_host.split("@"))[0]
      query = self.getOperation(user, "REVOKE ")
      query += ("ON %s.%s FROM %s;" % (db, tb, user_host))
      self._query_list.append(query)


def usage():
  print("usage : python mysql-usrman.py --table [TableName] --user [user]")
  print("Addional option :")
  print("\t--db [DatabaseName] : default is test")
  print("\t--region [us-west-1 or us-west-2] : default is us-west-1")
  print("\t--type [add or del] : default is add(GRANT)")
  print("ex1) python mysql-usrman.py --table helloworld --user web")
  print("ex2) python mysql-usrman.py --table helloworld --user was --region us-west-2")
  print("ex3) python mysql-usrman.py --table helloworld --servic was --priv del")

  inst = Instructor()
  users = "California(us-west-1) users :"
  for i in inst._db_ca.keys():
    users += " " + str(i)
  print(users)
  users = "Oregon(us-west-2) users :"
  for i in inst._db_og.keys():
    users += " " + str(i)
  print(users)


if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option("--table", help="table")
  parser.add_option("--user", help="user")
  parser.add_option("--db", help="database", default="test")
  parser.add_option("--region", help="region", default="us-west-1")
  parser.add_option("--priv", help="priv", default="add")

  (options, args) = parser.parse_args()
  if not options.table or not options.user:
    usage()
    sys.exit(1)

  options.priv = options.priv.lower()
  if options.priv == "add":
    granter = Granter()
  elif options.priv == "del":
    granter = Revoker()
  else:
    usage()
    sys.exit(1)
    
  if not granter.checkValidation(options.user, options.region):
    print("User '%s' does not exist in '%s'.\n" % (options.user, options.region))
    usage()
    sys.exit(1)

  granter.grant(options.table, options.user, options.db, options.region)