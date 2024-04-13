@{%
/* eslint-disable*/
%}
@builtin "whitespace.ne"
@builtin "number.ne"

@{%
function opExpr(operator) {
  return d => ({
    type: 'operator',
    operator: operator,
    left: d[0],
    right: d[2]
  });
}

function opExprWs(operator) {
  return d => ({
    type: 'operator',
    operator: operator,
    left: d[0],
    right: d[4]
  });
}

function notOp(d) {
  return {
    type: 'not',
    operand: d[1]
  };
}
%}

main -> expr {% d => d[0] %}

expr ->
    pre_expr OR post_boolean_primary {% opExpr('or') %}
  | pre_expr "||" post_boolean_primary {% opExpr('or') %}
  | pre_expr XOR post_boolean_primary {% opExpr('xor') %}
  | pre_expr AND post_boolean_primary {% opExpr('and') %}
  | pre_expr "&&" post_boolean_primary {% opExpr('and') %}
  | NOT post_boolean_primary {% notOp %}
  | "!" post_boolean_primary {% notOp %}
  | pre_boolean_primary IS (__ NOT | null) __ (TRUE | FALSE)
  | boolean_primary {% d => d[0] %}

pre_expr ->
    expr __ {% d => d[0] %}
  | "(" _ expr _ ")" {% d => d[2] %}

post_expr ->
    __ expr {% d => d[1] %}
  | "(" _ expr _ ")" {% d => d[2] %}

mid_expr ->
    "(" _ expr _ ")" {% d => d[2] %}
  | __ "(" _ expr _ ")" {% d => d[3] %}
  | "(" _ expr _ ")" __ {% d => d[2] %}
  | __ expr __ {% d => d[1] %}

boolean_primary ->
    pre_boolean_primary IS (__ NOT | null) __ NULL {% d => ({
			type: 'is_null',
			not: (d[2] || []).length > 0,
			value:d[0]
		}) %}
  | boolean_primary "<=>" predicate {% opExpr('<=>') %}
  | boolean_primary _ comparison_type _ predicate {% d => (opExpr(d[2]))([d[0], null, d[4]]) %}
  | predicate {% d => d[0] %}

pre_boolean_primary ->
    "(" _ boolean_primary _ ")" {% d => d[2] %}
  | boolean_primary __ {% d => d[0] %}

post_boolean_primary ->
    "(" _ boolean_primary _ ")" {% d => d[2] %}
  | __ boolean_primary {% d => d[1] %}

comparison_type ->
    "=" {% d => d[0] %}
  | "<>" {% d => d[0] %}
  | "<" {% d => d[0] %}
  | "<=" {% d => d[0] %}
  | ">" {% d => d[0] %}
  | ">=" {% d => d[0] %}
  | "!=" {% d => d[0] %}

predicate ->
    in_predicate {% d => d[0] %}
  | between_predicate {% d => d[0] %}
  | like_predicate {% d => d[0] %}
  | bit_expr_predicate {% d => d[0] %}

in_predicate ->
 	  pre_bit_expr (NOT __ | null) IN _ "(" _ expr_comma_list _ ")" {% d => ({
      type: 'in',
      value: d[0],
      not: (d[1] || []).length > 0,
      exprs: (d[6].exprs || [])
    }) %}

between_predicate ->
    pre_bit_expr (NOT __ | null) BETWEEN mid_bit_expr AND post_bit_expr {%
      d => ({
        type: 'between',
        value: d[0],
        not: (d[1] || []).length > 0,
        lower: d[3],
        upper: d[5]
      })
    %}

bit_expr_predicate ->
		bit_expr {% d => d[0] %}
	| NOT __ bit_expr {% d => notOp(d.slice(1)) %}

mid_bit_expr ->
    "(" _ bit_expr _ ")" {% d => d[2] %}
  | __ "(" _ bit_expr _ ")" {% d => d[3] %}
  | "(" _ bit_expr _ ")" __ {% d => d[2] %}
  | __ bit_expr __ {% d => d[1] %}

like_predicate ->
    pre_bit_expr (NOT __ | null) LIKE post_bit_expr {%
      d => ({
        type: 'like',
        not: (d[1]||[]).length > 0,
        value: d[0],
        comparison: d[3]
      })
    %}

pre_bit_expr ->
    bit_expr __ {% d => d[0] %}
  | "(" _ bit_expr _ ")" {% d => d[2] %}

post_bit_expr ->
    __ bit_expr {% d => d[1] %}
  | "(" _ bit_expr _ ")" {% d => d[2] %}

bit_expr -> math {% d => d[0] %}
  # | simple_expr {% d => d[0] %}

math -> sum {% d => d[0] %}
sum ->
    sum _ "+" _ product {% opExprWs('+') %}
	| sum _ "-" _ product {% opExprWs('-') %}
	| product {% d => d[0] %}

product ->
	  product _ "/" _ simple_expr {% opExprWs('/') %}
  | product _ "*" _ simple_expr {% opExprWs('*') %}
  # | pre_bit_expr DIV post_simple_expr {% opExpr('DIV') %}
  # | pre_bit_expr MOD post_simple_expr {% opExpr('MOD') %}
  # | bit_expr _ "%" _ simple_expr {% opExprWs('%') %}
  # | bit_expr _ "^" _ simple_expr {% opExprWs('^') %}
  # | bit_expr _ "+" _ interval_expr {% opExprWs('+') %}
  # | bit_expr _ "-" _ interval_expr {% opExprWs('-') %}
  # | interval_expr {% d => d[0] %}
  | simple_expr {% d => d[0] %}

simple_expr ->
    literal {% d => d[0] %}
	# | interval_expr {% d => d[0] %}z
  | identifier {% d => ({type: 'column', name: d[0].value}) %}
	| distinct_expr {% d => d[0] %}
	| date_expr {% d => d[0] %}
	| now_expr {% d => d[0] %}
  | function_call {% d => d[0] %}
  | if_statement {% d => d[0] %}
	| ifnull_expr {% d => d[0] %}
  | "(" _ expr_comma_list _ ")" {% d => d[2] %}
  | case_statement {% d => d[0] %}
  | cast_statement {% d => d[0] %}
  # | convert_statement {% d => d[0] %}
  | identifier "." identifier {% d => ({type: 'column', table: d[0].value, name: d[2].value}) %}

post_simple_expr ->
    __ simple_expr {% d => d[1] %}
  | "(" _ simple_expr _ ")" {% d => d[2] %}

literal ->
    string {% d => d[0] %}
  | decimal {% d => ({type: 'decimal', value: d[0]}) %}
  | NULL {% d => ({type: 'null'}) %}
  | TRUE {% d => ({type: 'true'}) %}
  | FALSE {% d => ({type: 'false'}) %}

expr_comma_list ->
    expr {% d => d[0] %}
  | expr_comma_list _ "," _ expr {% d => {
			if(d[0].type == 'expr_comma_list') {
				return {
					type: 'expr_comma_list',
					exprs: d[0].exprs.concat(d[4])
				};
			}
			return ({type:'expr_comma_list', exprs: [d[0],d[4]]});
		} %}

if_statement ->
    IF _ "(" _ expr _ "," _ expr _ "," _ expr _ ")" {%
      d => ({
        type: 'if',
        condition: d[4],
        then: d[8],
        'else': d[12]
      })
    %}

case_statement ->
    CASE (__ | mid_expr) when_statement_list (__ ELSE __ expr __ | __) END {%
      d => ({
        type: 'case',
        match: d[1][0],
        when_statements: d[2].statements,
        'else': (d[3]||[])[3]
      })
    %}

when_statement_list ->
    when_statement {% d => ({statements: [d[0]]}) %}
  | when_statement_list __ when_statement {% d => ({
      statements: (d[0].statements||[]).concat([d[2]])
    })
  %}

when_statement ->
    WHEN __ expr __ THEN __ expr {%
      d => ({
        type: 'when',
        condition: d[2],
        then: d[6]
      })
    %}

# convert_statement ->
#     CONVERT _ "(" expr __ USING __ identifier ")" {%
#       d => ({
#         type: 'convert',
#         value: d[2],
#         using: d[4]
#       })
#     %}

ifnull_expr ->
		"ifnull"i _ "(" _ ")" {% d => ({type:'ifnull'}) %}
	|	"ifnull"i _ "(" _ expr _ "," _ expr_comma_list _ ")" {%
			d => ({
				type: 'ifnull',
				value: d[4],
				rest: d[8]
			})
		%}

@{%
	function dateExprFn(operator,left_index,interval_index) {
		return d => ({
			type: 'date_expr',
			operator,
			left_side: d[left_index],
			interval: d[interval_index]
		});
	}
%}

date_expr ->
		"date_add"i _ "(" _ expr _ "," _ interval_expr _ ")" {% dateExprFn('+',4,8) %}
	| "date_sub"i _ "(" _ expr _ "," _ interval_expr _ ")" {% dateExprFn('-',4,8) %}
	| expr _ "+" _ interval_expr {% dateExprFn('+',0,4) %}
	| expr _ "-" _ interval_expr {% dateExprFn('-',0,4) %}

interval_expr ->
   INTERVAL __ expr __ date_unit {%
     d => ({
         type: 'interval',
         value: d[2],
         unit: d[4]
       })
     %}

now_expr ->
		"now"i _ "()" {% d => ({type:'now'}) %}
	| "now"i _ "(" _ ")" {% d => ({type:'now'}) %}
	| "getdate"i _ "()" {% d => ({type:'now'}) %}
	| "getdate"i _ "(" _ ")" {% d => ({type:'now'}) %}

cast_statement ->
    CAST _ "(" _ expr __ AS __ data_type _ ")" {%
      d => ({
        type: 'cast',
        value: d[4],
        data_type: d[8]
      })
    %}

CAST -> "cast"i
AS -> "as"i

@{%
	function dataTypeFn(d) {
		return {
			type: 'data_type',
			data_type: d[0].toLowerCase()
		}
	}
%}
data_type ->
		"date"i {% dataTypeFn %}
	|	"datetime"i {% dataTypeFn %}
	|	"time"i {% dataTypeFn %}
	|	"char"i {% dataTypeFn %}
	|	"signed"i {% dataTypeFn %}
	|	"unsigned"i {% dataTypeFn %}
	|	"binary"i {% dataTypeFn %}

distinct_expr -> DISTINCT __ simple_expr {% d => ({
		type:'distinct',
		operand: d[2]
	}) %}

function_call ->
  	function_identifier _ "(" _ "*" _ ")" {% d => ({
      type:'function_call',
      name: d[0],
      select_all: true
    }) %}
  # | function_identifier _ "(" _ DISTINCT __ column _ ")" {% d => ({
  #     type: 'function_call',
  #     name: d[0],
  #     distinct: true,
  #     parameters: [d[6]]
  #   })%}
  # | function_identifier _ "(" _ ALL post_expr _ ")" {% d => ({
  #     type: 'function_call',
  #     name: d[0],
  #     all: true,
  #     parameters: [d[5]]
  #   })%}
  | function_identifier _ "()" {% d => ({
      type: 'function_call',
      name: d[0],
      parameters: []
    })%}
  | function_identifier _ "(" _ expr_comma_list _ ")" {% d => ({
      type: 'function_call',
      name: d[0],
      parameters: (d[4].exprs || [d[4]])
    })%}

string ->
    dqstring {% d => ({type: 'string', string: d[0]}) %}
  | sqstring {% d => ({type: 'string', string: d[0]}) %}

# column ->
#     identifier {% d => ({type: 'column', name: d[0].value}) %}
  # | identifier __ AS __ identifier {% d => ({type: 'column', name: d[0].value, alias: d[2].value}) %}

identifier ->
    btstring {% d => ({type: 'identifier', value:d[0]}) %}
  | "[" ([^\]] | "\\]"):+ "]" {% d => ({type: 'identifier', value: d[1].map(x => x[0]).join('')}) %}
  | [a-zA-Z_] [a-zA-Z0-9_]:* {% (d,l,reject) => {
    const value = d[0] + d[1].join('');
    if(['NULL','TRUE','FALSE'].indexOf(value.toUpperCase()) != -1) return reject;
    return {type: 'identifier', value: value};
  } %}

function_identifier ->
    btstring {% d => ({value:d[0]}) %}
  | [a-zA-Z_] [a-zA-Z0-9_]:* {% (d,l,reject) => {
    const value = d[0] + d[1].join('');
		if(value.toLowerCase() == 'if') return reject;
		if(value.toLowerCase() == 'ifnull') return reject;
    //if(reserved.indexOf(value.toUpperCase()) != -1 && valid_function_identifiers.indexOf(value.toUpperCase()) == -1) return reject;
		// if(functions.indexOf(value.toUpperCase()) == -1) return reject;
    return {value: value};
  } %}

DISTINCT -> "distinct"i
INTERVAL -> "interval"i

NULL -> [nN] [uU] [lL] [lL]

AND -> [aA] [nN] [dD]
OR -> [oO] [rR]
XOR -> [xX] [oO] [rR]
NOT -> [nN] [oO] [tT]

TRUE -> [tT] [rR] [uU] [eE]
FALSE -> [fF] [aA] [lL] [sS] [eE]

IS -> [iI] [sS]
IN -> [iI] [nN]

BETWEEN -> [bB] [eE] [tT] [wW] [eE] [eE] [nN]
LIKE -> [lL] [iI] [kK] [eE]

IF -> [iI] [fF]
ELSE -> [eE] [lL] [sS] [eE]
CASE -> [cC] [aA] [sS] [eE]
WHEN -> [wW] [hH] [eE] [nN]
THEN -> [tT] [hH] [eE] [nN]
END -> [eE] [nN] [dD]

date_unit ->
	  "microsecond"i
  | "second"i
  | "minute"i
  | "hour"i
  | "day"i
  | "week"i
  | "month"i
  | "quarter"i
  | "year"i

### Copied & modified from builtin

dqstring -> "\"" dstrchar:* "\"" {% function(d) {return d[1].join(""); } %}
sqstring -> "'"  sstrchar:* "'"  {% function(d) {return d[1].join(""); } %}
btstring -> "`"  [^`]:*    "`"  {% function(d) {return d[1].join(""); } %}

dstrchar -> [^\\"\n] {% id %}
    | "\\" strescape {%
      function(d) {
        return JSON.parse("\""+d.join("")+"\"");
      }
      %}

sstrchar -> [^\\'\n] {% id %}
    | "\\" strescape {%
      function(d) {
        return JSON.parse("\""+d.join("")+"\"");
      } %}
    | "\\'"
        {% function(d) {return "'"; } %}

strescape -> ["\\/bfnrt] {% id %}
    | "u" [a-fA-F0-9] [a-fA-F0-9] [a-fA-F0-9] [a-fA-F0-9] {%
    function(d) {
        return d.join("");
    }
%}
