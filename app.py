import sqlparse
import re
from flask import Flask, render_template, request
from sqlparse.sql import Where, Parenthesis

app = Flask(__name__)

class RobustSQLToRATranslator:
    def __init__(self):
        self.symbols = {
            'SELECT': 'π', 'WHERE': 'σ', 'JOIN': '⨝',
            'UNION': '∪', 'INTERSECT': '∩', 'EXCEPT': '−', 'MINUS': '−',
            'IN': '⋉', 'EXISTS': '⋉', 'NOT IN': '▷', 'NOT EXISTS': '▷'
        }

    def translate(self, sql):
        sql = sql.strip().strip(';').replace('\n', ' ')
        
        # Handle wrapped Set Operations
        if sql.startswith('(') and sql.endswith(')'):
            if sql.count('SELECT') > 1 and any(op in sql.upper() for op in ['UNION', 'INTERSECT', 'EXCEPT']):
                sql = sql[1:-1].strip()

        parsed = sqlparse.parse(sql)[0]
        tokens = [t for t in parsed.tokens if not t.is_whitespace]
        
        # 1. Handle Set Operations
        for i, token in enumerate(tokens):
            val = token.value.upper()
            if val in ['UNION', 'INTERSECT', 'EXCEPT', 'MINUS']:
                left_sql = " ".join([t.value for t in tokens[:i]])
                right_sql = " ".join([t.value for t in tokens[i+1:]])
                return f"({self.translate(left_sql)} {self.symbols[val]} {self.translate(right_sql)})"

        return self._build_ra_tree(tokens)

    def _build_ra_tree(self, tokens):
        relation = self._extract_relation(tokens)
        selection = self._extract_selection(tokens)
        projection = self._extract_projection(tokens)

        res = relation
        if selection:
            res = f"{selection}({res})"
        if projection and projection != "π_{*}":
            res = f"{projection}({res})"
        return res

    def _extract_relation(self, tokens):
        try:
            from_idx = next(i for i, t in enumerate(tokens) if t.value.upper() == 'FROM')
            rel_tokens = []
            for t in tokens[from_idx+1:]:
                if isinstance(t, Where) or t.value.upper() in ['GROUP', 'ORDER', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT']:
                    break
                rel_tokens.append(t)
            
            full_from = "".join([t.value for t in rel_tokens]).strip()
            
            if full_from.startswith('('):
                match = re.search(r'\((.*)\)', full_from, re.DOTALL)
                if match: return f"({self.translate(match.group(1))})"
            
            if 'JOIN' in full_from.upper():
                return self._parse_joins(full_from)
                
            return full_from
        except: return "∅"

    def _parse_joins(self, join_str):
        parts = re.split(r'\s+JOIN\s+', join_str, flags=re.IGNORECASE)
        base = parts[0].strip()
        for p in parts[1:]:
            if ' ON ' in p.upper():
                table_cond = re.split(r'\s+ON\s+', p, flags=re.IGNORECASE)
                table = table_cond[0].strip()
                condition = table_cond[1].strip()
                base = f"({base} ⨝_{{{condition}}} {table})"
            else:
                base = f"({base} ⋈ {p.strip()})"
        return base

    def _extract_selection(self, tokens):
        where_clause = next((t for t in tokens if isinstance(t, Where)), None)
        if not where_clause: return ""

        raw_where = where_clause.value.strip()
        if raw_where.upper().startswith('WHERE'):
            raw_where = raw_where[5:].strip()

        if any(x in raw_where.upper() for x in ['IN', 'EXISTS']):
            return self._handle_complex_where(where_clause)
            
        return f"σ_{{{raw_where}}}"

    def _handle_complex_where(self, where_node):
        tokens = [t for t in where_node.tokens if not t.is_whitespace]
        for i, t in enumerate(tokens):
            if isinstance(t, Parenthesis):
                inner_sql = t.value[1:-1]
                op = "⋉" 
                context = " ".join([tok.value.upper() for tok in tokens[max(0, i-2):i]])
                if "NOT" in context:
                    op = "▷"
                return f"{op}_{{{self.translate(inner_sql)}}}"
        return f"σ_{{{where_node.value}}}"

    def _extract_projection(self, tokens):
        if tokens and tokens[0].value.upper() == 'SELECT':
            return f"π_{{{tokens[1].value}}}"
        return ""

translator = RobustSQLToRATranslator()

@app.route('/', methods=['GET', 'POST'])
def index():
    ra_result = ""
    sql_query = ""
    if request.method == 'POST':
        # Get data from the form
        sql_query = request.form.get('sql_query', '').strip()
        if sql_query:
            try:
                ra_result = translator.translate(sql_query)
            except Exception as e:
                ra_result = f"Error: {str(e)}"
        else:
            ra_result = "Please enter a query."
            
    return render_template('index.html', ra_result=ra_result, sql_query=sql_query)

if __name__ == '__main__':
    # Using port 5000 is standard, but you can change it if you still see 405
    app.run(debug=True)