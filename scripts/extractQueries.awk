BEGIN {
  map["0x0"] = "nothing"   # just an example
}

/{requests}.*h.*-request-begin.*_api\/cursor/ {
    nr = split($0, x, " ")
    s = x[7];
    for (i = 8; i <= nr; ++i) {
        s = s " " x[i]
    }
    nr = split(s, y, ",")
    this = y[2]
    url = y[5]
    where = match(url, /^"\/_db\/([^/]*)\//, z)
    if (where != 0) {
        map[this] = z[1]
    }
}

/{requests}.*h.*-request-body.*query/ {
    nr = split($0, x, " ");
    s = x[7];
    for (i = 8; i <= nr; ++i) {
        s = s " " x[i];
    }
    nr = split(s, y, ",");
    this = y[2]
    t = y[5]
    for (i = 6; i <= nr; ++i) {
        t = t "," y[i];
    }
    u = substr(t, 2, length(t)-2)
    gsub(/\\"/, "\"", u)
    gsub(/\\\\/, "\\", u)
    print "{\"t\":\"" $1 "\", \"db\": \"" map[this] "\", \"q\":" u "}" 
    delete map[this]
}
