# Boto3_Practice

程式碼要達成如下要求：
1.透過 AWS sdk 取得自己AWS帳戶上月2020/12帳單資料
2.將資料存入 DynamoDB (service,region,type,UnblendedCost,quantity 存入約10個欄位即可)
3.並將json檔案存於於 S3 bucket
4.將上列 json 檔案壓縮後存放於S3
5.提供 S3 presigned URL 下載上面壓縮後的檔案，兩天內可下載
