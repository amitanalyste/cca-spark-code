# practice with sqoop
### obar1

# using optionss-file 
sqoop --options-file sqoop.opt --table departments --target-dir 001  &>tmp.log

# columsn
sqoop --options-file sqoop.opt --table orders --target-dir 002  --columns  order_id,order_date  &>tmp.log

#where on fields
sqoop --options-file sqoop.opt --table orders --target-dir 003 where "order_id > 100"  &>tmp.log

# free query and split by
sqoop --options-file sqoop.opt  --query "select  * from orders where \$CONDITIONS"  --target-dir 004 --split-by order.id   &>tmp.log

# adding my where conditions
sqoop --options-file sqoop.opt   --query " select  * from orders where order_id>1000 and \$CONDITIONS"   --target-dir 005 --split-by order.id  &>tmp.log

#validate the copy
sqoop --options-file sqoop.opt   --table orders   --target-dir 006 --validate 

#append same folder
sqoop --options-file sqoop.opt --table departments --target-dir 001  --append
