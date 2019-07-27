# IMPORT LIBRARIES
library(sqldf)

ageCal <- function(lst){
  lst <- lst %/% 10000;
  lst <- 99 - lst;
}

sexCal <- function(lst){
  lst <- lst %/% 100;
  lst <- lst %% 100;
  lst <- lst <= 12;
}

# GET DATA FILES
account_df <- read.csv("https://query.data.world/s/szjie5too4l2gj5pzwjhgi2xjapux6", header=TRUE, stringsAsFactors=FALSE, sep=";");
card_df <- read.csv("https://query.data.world/s/xr47n3l5i4ykdizpo3xvsukuebdf7z", header=TRUE, stringsAsFactors=FALSE, sep=";");
client_df <- read.csv("https://query.data.world/s/tfaoqji3xkzy5g4zhs3aaub52gbe5m", header=TRUE, stringsAsFactors=FALSE, sep=";");
disp_df <- read.csv("https://query.data.world/s/beo64ydtarnaxzoa5cejtsivv2s4ol", header=TRUE, stringsAsFactors=FALSE, sep=";");
district_df <- read.csv("https://query.data.world/s/7j2onojk6pqx5szrqor5hltaasq34r", header=TRUE, stringsAsFactors=FALSE, sep=";");
loan_df <- read.csv("https://query.data.world/s/7pbggdhr32ki7aqk4u6h5yucilu5oa", header=TRUE, stringsAsFactors=FALSE, sep=";");
order_df <- read.csv("https://query.data.world/s/yviysxpbcc6uswhta22dm2bedacra6", header=TRUE, stringsAsFactors=FALSE, sep=";");
trans_df <- read.csv("https://query.data.world/s/x74iyfc47er6bwyb3jwotg6j6gznwc", header=TRUE, stringsAsFactors=FALSE, sep=";");


# CHOOSE NEEDED DATA
# loan table
loan_table <- loan_df[c(2,1,4,length(loan_df))];
colnames(loan_table) <- c("ac_id", 'loan_id','amount','status');
loan_table <- sqldf('SELECT * FROM loan_table GROUP BY ac_id');

# client table
client_table <- client_df[c(1,3,2)];
client_table <- sqldf('SELECT * FROM client_table GROUP BY client_id');
client_table <- data.frame(
  client_id = client_table[1],
  district_id = client_table[2],
  age = ageCal(client_table[3]),
  sex = sexCal(client_table[3])
)
colnames(client_table)[3] <- "age";
colnames(client_table)[4] <- "sex";

# transaction table
transaction_table = trans_df[c(2,3,4,6,7)];
colnames(transaction_table) <- c("ac_id","date_of_transaction","transaction_type","amount","bal_post_transaction");
transaction_table <- subset(
  transaction_table, 
  (transaction_table[2]%/%10000) > 97,
  select = c(1,2,3,4,5)
);
transaction_table <- sqldf('SELECT ac_id, SUM(amount), SUM(bal_post_transaction) FROM transaction_table GROUP BY ac_id');
colnames(transaction_table)[3] <- "SUM(balance)";

# card table
card_table <- card_df[c(2,3)];
colnames(card_table) <- c("disposition_id","card_type");
card_table <- sqldf('SELECT * FROM card_table GROUP BY disposition_id');

# district table
district_table <- district_df[c(1,2,3,11,12,13,14)];
colnames(district_table) <- c("district_id","district_name","region","avg_salary","unemp_rate_95","unemp_rate_96","entrepreneur_per_1000");
district_table <- sqldf('SELECT * FROM district_table GROUP BY district_id');
district_table <- subset(
  district_table,
  unemp_rate_95 > 0 & unemp_rate_96 > 0,
  select = c(1:length(district_table))
);
district_table$unemp_rate_95 <- as.numeric(district_table$unemp_rate_95);
district_table[5] = abs(district_table[5] - district_table[6]);
colnames(district_table)[5] <- "unemp_diff_95_96";
district_table <- district_table[,-6];

# disposition table
disposition_table <- disp_df[c(3,1,4,2)];
colnames(disposition_table) <- c("account_id","disp_id","type","client_id");
disposition_table <- sqldf('SELECT * FROM disposition_table GROUP BY disp_id');


# JOIN TABLES
gr_disp_client <- merge(disposition_table, client_table, by="client_id");
colnames(gr_disp_client)[3] <- "disposition_id";
gr_disp_client_card <- merge(gr_disp_client, card_table, by="disposition_id");
gr_disp_client_card_district <- merge(gr_disp_client_card, district_table, by="district_id");
colnames(gr_disp_client_card_district)[4] <- "ac_id";
gr_disp_client_card_district_transaction <- merge(gr_disp_client_card_district, transaction_table, by="ac_id");
gr_disp_client_card_district_transaction_loan <- merge(gr_disp_client_card_district_transaction, loan_table, by="ac_id");
final_table <- gr_disp_client_card_district_transaction_loan[c(1,5,6,7,8,9,11,12,13,14,17,18)];
colnames(final_table) <- c("ac_id", "disp_type", "age", "sex", "card_type", "dist_name", "avg_sal", "unemp_rate", "no_of_entre", "transaction_sum", "loan_amount", "loan_status");


# CLASSIFICATION
good_client <- subset(
  final_table,
  transaction_sum > 1000000 & avg_sal > 10000 & loan_status == "A" & (age <= 65 & age > 25),
  select = c(1:length(final_table))
);

normal_client <- subset(
  final_table,
  transaction_sum < 1000000 & transaction_sum > 150000 & avg_sal > 6000 & (loan_status == "A" | loan_status == "C") & (age <= 55 & age >= 25) & unemp_rate < 0.8,
  select = c(1:length(final_table))
);

risky_client <- subset(
  final_table,
  avg_sal > 6000 & (loan_status == "B" | loan_status == "D") & age > 35 & no_of_entre > 100,
  select = c(1:length(final_table))
);

# SAVE RESULTS
write.table(good_client, "good_client.csv", append = FALSE, sep = " ", dec = ".", row.names = FALSE, col.names = TRUE);
write.table(normal_client, "normal_client.csv", append = FALSE, sep = " ", dec = ".", row.names = FALSE, col.names = TRUE);
write.table(risky_client, "risky_client.csv", append = FALSE, sep = " ", dec = ".", row.names = FALSE, col.names = TRUE);
