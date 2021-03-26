dftype <- c();
dfvalue <- c();

cuts <- c(0, 0.01, 0.05, 0.1, 0.2);

get_rows_pct <- function(vec, pctStart, pctEnd) {
  s <- sum(vec);
  start <- 0, end <- 0;

  for (i in 1:length(vec)) {
    if (sum(vec[1:i]) >= pctStart * s) {
      start <- i;
      break
    }
  }

  for (i in 1:length(vec)) {
    if (sum(vec[1:i]) >= pctEnd * s) {
      end <- i;
      break
    }
  }

  start:end
}

for (lg in 1:1000) {
  filename <- paste0("/home/jhli/git_repos/tmpTosScripts/scripts/result/ali_lbas_freqs_cv/ali_lbas_freqs_cv/", lg, ".data");
  if (!file.exists(filename)) {
    next
  }

  if (lg %% 100 == 0) {
    print(filename);
  }

  x1 <- read.table(filename, header = F, stringsAsFactors = F);
  names(x1) <- c("freq", "cN", "n", "sqrSum", "sum", "avg", "sd", "cv");
  x1 <- x1[order(-x1$freq), ];
  uwb <- sum(x1$n);


  for (i in 2:length(cuts)) {
    rs <- get_rows_pct(x1$n, cuts[i], cuts[i-1]);
    n <- sum(x1$n[rs]);
    sqrSum <- sum(x1$sqrSum[rs]);
    s <- sum(x1$sum[rs]);
    
    avg <- s / n;
    sd <- (sqrSum / (n-1) - 2 * avg / (n-1) * s + n / (n-1) * avg * avg)^(0.5);
    cv <- sd / avg;

    dftype <- c(dftype, ifelse(i == 2, "top 1%", paste0("top ", cuts[i-1]*100, "-", cuts[i]*100)));
    dfvalue <- c(dfvalue, cv);
  }
}

df <- data.frame(type = dftype, value = dfvalue);
types <- unique(df$type);
df$type <- factor(df$type, levels = types);

for (tp in types) {
  subs <- subset(df, type == tp);
  print(tp);
  print(quantile(subs$value, c(0.25, 0.5, 0.75)));
}
