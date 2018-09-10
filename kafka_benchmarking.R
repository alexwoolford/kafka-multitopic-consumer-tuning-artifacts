rm(list = ls())
library(RMySQL)
library(ggplot2)
library(plyr)
library(scales)

con <- dbConnect(RMySQL::MySQL(), dbname = "kafka_benchmark", user = "root", password = "V1ctoria", host = "deepthought")
data <- dbReadTable(con, "kafka_benchmark")
dbDisconnect(con)

data <- mutate(data, messages_per_second = message_count / seconds)

model.lm <- lm(messages_per_second ~ log(max_batch_size) + partitions + threads, data = data)
summary(model.lm)

data <- mutate(data, max_batch_size_text = paste("batch_size: ", max_batch_size))
data <- mutate(data, partitions_text = paste("partitions: ", partitions))

# if there's just one partition, it doesn't make sense to increase the number of threads
qplot(threads, messages_per_second, data = data) + facet_grid(max_batch_size_text~partitions_text) + geom_line() + scale_y_continuous(labels=comma) + 
  geom_vline(data=subset(data, partitions==1), aes(xintercept=1), colour="blue", linetype="dotdash") + 
  geom_vline(data=subset(data, partitions==2), aes(xintercept=2), colour="blue", linetype="dotdash") +
  geom_vline(data=subset(data, partitions==3), aes(xintercept=3), colour="blue", linetype="dotdash") +
  geom_vline(data=subset(data, partitions==4), aes(xintercept=4), colour="blue", linetype="dotdash") +
  geom_vline(data=subset(data, partitions==5), aes(xintercept=5), colour="blue", linetype="dotdash")

