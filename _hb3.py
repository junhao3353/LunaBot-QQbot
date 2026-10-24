import io
p = r'D:\py\XM\QQbot\webui-dashboard.html'
h = io.open(p, 'r', encoding='utf-8').read()
h = h.replace(
    "#hamburger { display:none; position:fixed; top:80px; left:12px; z-index:1060;",
    "#hamburger { display:none; position:fixed; top:12px; left:12px; z-index:1060;"
)
io.open(p, 'w', encoding='utf-8', newline='\n').write(h)
print("ok")
