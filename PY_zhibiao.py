#制表排版
d = { 'Adam': 95, 'Lisa': 98, 'Bart': 100 }
def generate_tr(name, score):
    if score > 90:
        return '<tr><td>%s</td><td style="color:red">%s</td></tr>' % (name, score)
    return '<tr><td>%s</td><td>%s</td></tr>' % (name, score)
tds = [generate_tr(name, score) for name, score in d.items()]
print('<table border="1">')
print ('<tr><th>Name</th><th>Score</th><tr>')
print ('\n'.join(tds))
print ('</table>')


