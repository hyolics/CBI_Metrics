# CBI_Metrics  
Status: in process  
Goal: Aggregate不同DB(AWS, MySQL, MongoDB, Postgresql) UI Log data， CBI = Customer Behavior Index，將Users進入產品的行為量化為Metrics.  
  
GetData.py: 從不同DB撈取資料  
QI.py: QI CBI  
SEP.py: SEP CBI  
main.py: 主程式，最後將結果分別upload到DB和sheet，並進行監控  
