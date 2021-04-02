options(scipen=999);

new_percentiles <- function(vec, value_col, cnt_col, cuts) {
  vec <- vec[order(vec[, value_col]), ];
  s <- sum(as.numeric(vec[, cnt_col]));
  ts <- 0;
  index <- 1;

  ret <- c();

  for (i in 1:nrow(vec)) {
    if (index > length(cuts)) {
      break
    }
    ts <- ts + as.numeric(vec[i, cnt_col]);

    while (index <= length(cuts) & ts > s * cuts[index]) {
      ret <- c(ret, vec[i, value_col]);
      print(paste0(cuts[index] * 100, " percentile: ", vec[i, value_col])); 
      print(paste0("ts = ", ts, ", s = ", s, ", i = ", i, ", nrow = ", nrow(vec)));
      index <- index + 1;
    }
  }

  ret
}

filenames <- paste0("../processed/ud/", c("ali", "tc", "msr"), "_total_ud_time.data");

for (fn in filenames) {
  print(paste0("Reading ", fn, " ..."));
  attr <- read.table(fn, header = T, stringsAsFactors = F);
  attr$timeInSec <- attr$timeIn100ms / 10;
  print(paste0("Read ", fn, " finished"));
  print(new_percentiles(attr, "timeInSec", "cnts", c(0.25, 0.5, 0.75, 0.9, 0.95)));
}

q()

# 3. RAW, WAW
###### CDF of Update distance 

proc <- function(data_attr, name) {
  ret <- data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = F);
  names(ret) <- c("log", "name", "type", "ud");

  for (lg in unique(data_attr$log)) {
    t <- subset(data_attr, log == lg); 
    mn <- new_mean(t, "timeInSec", "cnts");
    if (is.nan(mn)) {
      next
    }
    cuts <- c(0.25, 0.50, 0.75, 0.9, 0.95);
    pcts <- new_percentiles(t, "timeInSec", "cnts", cuts);

#    ret[nrow(ret) + 1, ] <- list(lg, name, "Mean", mn);
    for (i in 1:length(cuts)) {
      ret[nrow(ret) + 1, ] <- list(lg, name, paste0(as.character(cuts[i]*100), "th"), pcts[i]);
    }
  }

  ret;
}

dataS1 <- proc(tc_attr, "Tencent");
#dataS2 <- proc(msr_attr, "MSRC");
#dataS3 <- proc(kdev_attr, "AliCloud");

xscale <- seq(0, 1.5, 0.25);  # second
xlabels <- xscale * 24; 
xlimits <- c(0, 1.5);

proc <- function(data_attr, val_col, cuts) {
  data_attr$cnts <- 1;
  pcts <- new_percentiles(data_attr, val_col, "cnts", cuts);
#  print(cuts);
#  print(pcts);
}

numPrinted <- 1;
myplot <- function(data_attr, name, yscale, ylabels, ylimits) {
  xlab_name <- paste0(name);
  ylab_name <- "Update intervals";
  data <- data_attr;
  types <- unique(data$type);
  data$type <- factor(data$type, types);

  for (type0 in types) {
    t <- subset(data, type == type0);
#    proc(t, "ud", c(0.01, 0.25, 0.5, 0.75, 0.99));
  }

  coef <- 1;
  print(paste0("OK ? f14_", name, ".csv"));

  print(nrow(data));
  write.table(data[, 2:4], file = paste0("f14_", name, ".csv"), quote = F, row.names = F, col.names = T, sep = ',');

  t <- ggplot(data = data, aes(x = type, y = ud)) + 
    geom_boxplot() + 
    coord_cartesian(ylim = ylimits) +           
    scale_y_continuous(breaks = yscale, labels = ylabels, trans = 'log10') + 
    ylab(ylab_name) + xlab(xlab_name) +
    simplifiedTheme(c(0.85, 0.35), axis.text.size = 12, legend.text.size = 12, legend.direction = "vertical");

  t
}

yscale <- c(1, 60, 3600, 86400, 86400*7, 86400*90);
ylabels <- c("1s", "1min", "1h", "24h", "7d", "90d");
ylimits <- c(1, 86400*90);
simplePdf("f13-tc", mywidth, myheight, T);
print(myplot(dataS1, "Tencent", yscale, ylabels, ylimits));
#simplePdf("f13-msr", mywidth, myheight, T);
#print(myplot(dataS2, "MSRC", yscale, ylabels, ylimits));
#simplePdf("f13-kdev", mywidth, myheight, T);
#print(myplot(dataS3, "AliCloud", yscale, ylabels, ylimits));

proc2 <- function(data_attr, name) {
  ret <- data.frame(matrix(ncol = 4, nrow = 0), stringsAsFactors = F);
  names(ret) <- c("log", "name", "type", "pcts");

  for (lg in unique(data_attr$log)) {
    t <- subset(data_attr, log == lg); 
    cuts <- c(300, 1800, 4*3600, Inf);
    parts <- 1:4;

    sum_cnts <- sum(t$cnts);
    pcts <- c();
    flag <- F;
    for (cut in cuts) {
      t0 <- subset(t, timeInSec <= cut);
      pct <- sum(t0$cnts) / sum_cnts;
      if (is.nan(pct)) {
        flag <- T;
        next
      }
      if (length(pcts) > 0) {
        pct <- pct - sum(pcts);
      }
      pcts <- c(pcts, pct);
    }

    if (flag) {
      next
    }

    for (i in 1:length(cuts)) {
      ret[nrow(ret) + 1, ] <- list(lg, name, paste0(as.character(parts[i])), pcts[i]);
    }
  }

  ret;
}

dataS1 <- proc2(tc_attr, "Tencent");
#dataS2 <- proc2(msr_attr, "MSRC");
#dataS3 <- proc2(kdev_attr, "AliCloud");

proc <- function(data_attr, val_col, cuts) {
  data_attr$cnts_1 <- 1;
  pcts <- new_percentiles(data_attr, val_col, "cnts_1", cuts);
  print(cuts);
  print(pcts);
}

myplot <- function(data_attr, name, yscale, ylabels, ylimits) {
  xlab_name <- "Ranges (min)";
  ylab_name <- "Percentage (%)";
  data <- data_attr;
  types <- unique(data$type);
  data$type <- factor(data$type, types);

  print(name);
  for (type0 in types) {
    t <- subset(data, type == type0);
#    proc(t, "pcts", c(0.25, 0.5, 0.75, 0.99));
  }

  coef <- 1;

  data$pcts <- round(data$pcts, digits = 4);
  print(paste0("f14_level_", name, ".csv"));
  print(nrow(data));
  write.table(data[, 2:4], file = paste0("f14_level_", name, ".csv"), quote = F, row.names = F, col.names = T, sep = ',');

  t <- ggplot(data = data, aes(x = type, y = pcts)) + 
    geom_boxplot() + 
    coord_cartesian(ylim = ylimits * coef) +           
    scale_x_discrete(breaks = types, labels = c("<5", "5-30", "30-240", ">240")) + 
    scale_y_continuous(breaks = yscale, labels = ylabels) + 
    ylab(ylab_name) + xlab(xlab_name) +
    simplifiedTheme(c(0.85, 0.35), axis.text.size = 12, legend.text.size = 12, legend.direction = "vertical");

  t
}

yscale <- seq(0, 1, 0.2);
ylabels <- yscale * 100;
ylimits <- c(0, 1.02);
simplePdf("f13-tc-level", mywidth, myheight, T);
print(myplot(dataS1, "Tencent", yscale, ylabels, ylimits));
#simplePdf("f13-msr-level", mywidth, myheight, T);
#print(myplot(dataS2, "MSRC", yscale, ylabels, ylimits));
#simplePdf("f13-kdev-level", mywidth, myheight, T);
#print(myplot(dataS3, "AliCloud", yscale, ylabels, ylimits));
