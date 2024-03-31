/* eslint-disable */
// Generated automatically by nearley, version 2.20.1
// http://github.com/Hardmath123/nearley
(function () {
function id(x) { return x[0]; }

/* eslint-disable*/


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


	function dateExprFn(operator,left_index,interval_index) {
		return d => ({
			type: 'date_expr',
			operator,
			left_side: d[left_index],
			interval: d[interval_index]
		});
	}


	function dataTypeFn(d) {
		return {
			type: 'data_type',
			data_type: d[0].toLowerCase()
		}
	}
var grammar = {
    Lexer: undefined,
    ParserRules: [
    {"name": "_$ebnf$1", "symbols": []},
    {"name": "_$ebnf$1", "symbols": ["_$ebnf$1", "wschar"], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "_", "symbols": ["_$ebnf$1"], "postprocess": function(d) {return null;}},
    {"name": "__$ebnf$1", "symbols": ["wschar"]},
    {"name": "__$ebnf$1", "symbols": ["__$ebnf$1", "wschar"], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "__", "symbols": ["__$ebnf$1"], "postprocess": function(d) {return null;}},
    {"name": "wschar", "symbols": [/[ \t\n\v\f]/], "postprocess": id},
    {"name": "unsigned_int$ebnf$1", "symbols": [/[0-9]/]},
    {"name": "unsigned_int$ebnf$1", "symbols": ["unsigned_int$ebnf$1", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "unsigned_int", "symbols": ["unsigned_int$ebnf$1"], "postprocess": 
        function(d) {
            return parseInt(d[0].join(""));
        }
        },
    {"name": "int$ebnf$1$subexpression$1", "symbols": [{"literal":"-"}]},
    {"name": "int$ebnf$1$subexpression$1", "symbols": [{"literal":"+"}]},
    {"name": "int$ebnf$1", "symbols": ["int$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "int$ebnf$1", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "int$ebnf$2", "symbols": [/[0-9]/]},
    {"name": "int$ebnf$2", "symbols": ["int$ebnf$2", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "int", "symbols": ["int$ebnf$1", "int$ebnf$2"], "postprocess": 
        function(d) {
            if (d[0]) {
                return parseInt(d[0][0]+d[1].join(""));
            } else {
                return parseInt(d[1].join(""));
            }
        }
        },
    {"name": "unsigned_decimal$ebnf$1", "symbols": [/[0-9]/]},
    {"name": "unsigned_decimal$ebnf$1", "symbols": ["unsigned_decimal$ebnf$1", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "unsigned_decimal$ebnf$2$subexpression$1$ebnf$1", "symbols": [/[0-9]/]},
    {"name": "unsigned_decimal$ebnf$2$subexpression$1$ebnf$1", "symbols": ["unsigned_decimal$ebnf$2$subexpression$1$ebnf$1", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "unsigned_decimal$ebnf$2$subexpression$1", "symbols": [{"literal":"."}, "unsigned_decimal$ebnf$2$subexpression$1$ebnf$1"]},
    {"name": "unsigned_decimal$ebnf$2", "symbols": ["unsigned_decimal$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "unsigned_decimal$ebnf$2", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "unsigned_decimal", "symbols": ["unsigned_decimal$ebnf$1", "unsigned_decimal$ebnf$2"], "postprocess": 
        function(d) {
            return parseFloat(
                d[0].join("") +
                (d[1] ? "."+d[1][1].join("") : "")
            );
        }
        },
    {"name": "decimal$ebnf$1", "symbols": [{"literal":"-"}], "postprocess": id},
    {"name": "decimal$ebnf$1", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "decimal$ebnf$2", "symbols": [/[0-9]/]},
    {"name": "decimal$ebnf$2", "symbols": ["decimal$ebnf$2", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "decimal$ebnf$3$subexpression$1$ebnf$1", "symbols": [/[0-9]/]},
    {"name": "decimal$ebnf$3$subexpression$1$ebnf$1", "symbols": ["decimal$ebnf$3$subexpression$1$ebnf$1", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "decimal$ebnf$3$subexpression$1", "symbols": [{"literal":"."}, "decimal$ebnf$3$subexpression$1$ebnf$1"]},
    {"name": "decimal$ebnf$3", "symbols": ["decimal$ebnf$3$subexpression$1"], "postprocess": id},
    {"name": "decimal$ebnf$3", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "decimal", "symbols": ["decimal$ebnf$1", "decimal$ebnf$2", "decimal$ebnf$3"], "postprocess": 
        function(d) {
            return parseFloat(
                (d[0] || "") +
                d[1].join("") +
                (d[2] ? "."+d[2][1].join("") : "")
            );
        }
        },
    {"name": "percentage", "symbols": ["decimal", {"literal":"%"}], "postprocess": 
        function(d) {
            return d[0]/100;
        }
        },
    {"name": "jsonfloat$ebnf$1", "symbols": [{"literal":"-"}], "postprocess": id},
    {"name": "jsonfloat$ebnf$1", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "jsonfloat$ebnf$2", "symbols": [/[0-9]/]},
    {"name": "jsonfloat$ebnf$2", "symbols": ["jsonfloat$ebnf$2", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "jsonfloat$ebnf$3$subexpression$1$ebnf$1", "symbols": [/[0-9]/]},
    {"name": "jsonfloat$ebnf$3$subexpression$1$ebnf$1", "symbols": ["jsonfloat$ebnf$3$subexpression$1$ebnf$1", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "jsonfloat$ebnf$3$subexpression$1", "symbols": [{"literal":"."}, "jsonfloat$ebnf$3$subexpression$1$ebnf$1"]},
    {"name": "jsonfloat$ebnf$3", "symbols": ["jsonfloat$ebnf$3$subexpression$1"], "postprocess": id},
    {"name": "jsonfloat$ebnf$3", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "jsonfloat$ebnf$4$subexpression$1$ebnf$1", "symbols": [/[+-]/], "postprocess": id},
    {"name": "jsonfloat$ebnf$4$subexpression$1$ebnf$1", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "jsonfloat$ebnf$4$subexpression$1$ebnf$2", "symbols": [/[0-9]/]},
    {"name": "jsonfloat$ebnf$4$subexpression$1$ebnf$2", "symbols": ["jsonfloat$ebnf$4$subexpression$1$ebnf$2", /[0-9]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "jsonfloat$ebnf$4$subexpression$1", "symbols": [/[eE]/, "jsonfloat$ebnf$4$subexpression$1$ebnf$1", "jsonfloat$ebnf$4$subexpression$1$ebnf$2"]},
    {"name": "jsonfloat$ebnf$4", "symbols": ["jsonfloat$ebnf$4$subexpression$1"], "postprocess": id},
    {"name": "jsonfloat$ebnf$4", "symbols": [], "postprocess": function(d) {return null;}},
    {"name": "jsonfloat", "symbols": ["jsonfloat$ebnf$1", "jsonfloat$ebnf$2", "jsonfloat$ebnf$3", "jsonfloat$ebnf$4"], "postprocess": 
        function(d) {
            return parseFloat(
                (d[0] || "") +
                d[1].join("") +
                (d[2] ? "."+d[2][1].join("") : "") +
                (d[3] ? "e" + (d[3][1] || "+") + d[3][2].join("") : "")
            );
        }
        },
    {"name": "main", "symbols": ["expr"], "postprocess": d => d[0]},
    {"name": "expr", "symbols": ["pre_expr", "OR", "post_boolean_primary"], "postprocess": opExpr('or')},
    {"name": "expr$string$1", "symbols": [{"literal":"|"}, {"literal":"|"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "expr", "symbols": ["pre_expr", "expr$string$1", "post_boolean_primary"], "postprocess": opExpr('or')},
    {"name": "expr", "symbols": ["pre_expr", "XOR", "post_boolean_primary"], "postprocess": opExpr('xor')},
    {"name": "expr", "symbols": ["pre_expr", "AND", "post_boolean_primary"], "postprocess": opExpr('and')},
    {"name": "expr$string$2", "symbols": [{"literal":"&"}, {"literal":"&"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "expr", "symbols": ["pre_expr", "expr$string$2", "post_boolean_primary"], "postprocess": opExpr('and')},
    {"name": "expr", "symbols": ["NOT", "post_boolean_primary"], "postprocess": notOp},
    {"name": "expr", "symbols": [{"literal":"!"}, "post_boolean_primary"], "postprocess": notOp},
    {"name": "expr$subexpression$1", "symbols": ["__", "NOT"]},
    {"name": "expr$subexpression$1", "symbols": []},
    {"name": "expr$subexpression$2", "symbols": ["TRUE"]},
    {"name": "expr$subexpression$2", "symbols": ["FALSE"]},
    {"name": "expr", "symbols": ["pre_boolean_primary", "IS", "expr$subexpression$1", "__", "expr$subexpression$2"]},
    {"name": "expr", "symbols": ["boolean_primary"], "postprocess": d => d[0]},
    {"name": "pre_expr", "symbols": ["expr", "__"], "postprocess": d => d[0]},
    {"name": "pre_expr", "symbols": [{"literal":"("}, "_", "expr", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "post_expr", "symbols": ["__", "expr"], "postprocess": d => d[1]},
    {"name": "post_expr", "symbols": [{"literal":"("}, "_", "expr", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "mid_expr", "symbols": [{"literal":"("}, "_", "expr", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "mid_expr", "symbols": ["__", {"literal":"("}, "_", "expr", "_", {"literal":")"}], "postprocess": d => d[3]},
    {"name": "mid_expr", "symbols": [{"literal":"("}, "_", "expr", "_", {"literal":")"}, "__"], "postprocess": d => d[2]},
    {"name": "mid_expr", "symbols": ["__", "expr", "__"], "postprocess": d => d[1]},
    {"name": "boolean_primary$subexpression$1", "symbols": ["__", "NOT"]},
    {"name": "boolean_primary$subexpression$1", "symbols": []},
    {"name": "boolean_primary", "symbols": ["pre_boolean_primary", "IS", "boolean_primary$subexpression$1", "__", "NULL"], "postprocess":  d => ({
        	type: 'is_null',
        	not: (d[2] || []).length > 0,
        	value:d[0]
        }) },
    {"name": "boolean_primary$string$1", "symbols": [{"literal":"<"}, {"literal":"="}, {"literal":">"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "boolean_primary", "symbols": ["boolean_primary", "boolean_primary$string$1", "predicate"], "postprocess": opExpr('<=>')},
    {"name": "boolean_primary", "symbols": ["boolean_primary", "_", "comparison_type", "_", "predicate"], "postprocess": d => (opExpr(d[2]))([d[0], null, d[4]])},
    {"name": "boolean_primary", "symbols": ["predicate"], "postprocess": d => d[0]},
    {"name": "pre_boolean_primary", "symbols": [{"literal":"("}, "_", "boolean_primary", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "pre_boolean_primary", "symbols": ["boolean_primary", "__"], "postprocess": d => d[0]},
    {"name": "post_boolean_primary", "symbols": [{"literal":"("}, "_", "boolean_primary", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "post_boolean_primary", "symbols": ["__", "boolean_primary"], "postprocess": d => d[1]},
    {"name": "comparison_type", "symbols": [{"literal":"="}], "postprocess": d => d[0]},
    {"name": "comparison_type$string$1", "symbols": [{"literal":"<"}, {"literal":">"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "comparison_type", "symbols": ["comparison_type$string$1"], "postprocess": d => d[0]},
    {"name": "comparison_type", "symbols": [{"literal":"<"}], "postprocess": d => d[0]},
    {"name": "comparison_type$string$2", "symbols": [{"literal":"<"}, {"literal":"="}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "comparison_type", "symbols": ["comparison_type$string$2"], "postprocess": d => d[0]},
    {"name": "comparison_type", "symbols": [{"literal":">"}], "postprocess": d => d[0]},
    {"name": "comparison_type$string$3", "symbols": [{"literal":">"}, {"literal":"="}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "comparison_type", "symbols": ["comparison_type$string$3"], "postprocess": d => d[0]},
    {"name": "comparison_type$string$4", "symbols": [{"literal":"!"}, {"literal":"="}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "comparison_type", "symbols": ["comparison_type$string$4"], "postprocess": d => d[0]},
    {"name": "predicate", "symbols": ["in_predicate"], "postprocess": d => d[0]},
    {"name": "predicate", "symbols": ["between_predicate"], "postprocess": d => d[0]},
    {"name": "predicate", "symbols": ["like_predicate"], "postprocess": d => d[0]},
    {"name": "predicate", "symbols": ["bit_expr_predicate"], "postprocess": d => d[0]},
    {"name": "in_predicate$subexpression$1", "symbols": ["NOT", "__"]},
    {"name": "in_predicate$subexpression$1", "symbols": []},
    {"name": "in_predicate", "symbols": ["pre_bit_expr", "in_predicate$subexpression$1", "IN", "_", {"literal":"("}, "_", "expr_comma_list", "_", {"literal":")"}], "postprocess":  d => ({
          type: 'in',
          value: d[0],
          not: (d[1] || []).length > 0,
          exprs: (d[6].exprs || [])
        }) },
    {"name": "between_predicate$subexpression$1", "symbols": ["NOT", "__"]},
    {"name": "between_predicate$subexpression$1", "symbols": []},
    {"name": "between_predicate", "symbols": ["pre_bit_expr", "between_predicate$subexpression$1", "BETWEEN", "mid_bit_expr", "AND", "post_bit_expr"], "postprocess": 
        d => ({
          type: 'between',
          value: d[0],
          not: (d[1] || []).length > 0,
          lower: d[3],
          upper: d[5]
        })
            },
    {"name": "bit_expr_predicate", "symbols": ["bit_expr"], "postprocess": d => d[0]},
    {"name": "bit_expr_predicate", "symbols": ["NOT", "__", "bit_expr"], "postprocess": d => notOp(d.slice(1))},
    {"name": "mid_bit_expr", "symbols": [{"literal":"("}, "_", "bit_expr", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "mid_bit_expr", "symbols": ["__", {"literal":"("}, "_", "bit_expr", "_", {"literal":")"}], "postprocess": d => d[3]},
    {"name": "mid_bit_expr", "symbols": [{"literal":"("}, "_", "bit_expr", "_", {"literal":")"}, "__"], "postprocess": d => d[2]},
    {"name": "mid_bit_expr", "symbols": ["__", "bit_expr", "__"], "postprocess": d => d[1]},
    {"name": "like_predicate$subexpression$1", "symbols": ["NOT", "__"]},
    {"name": "like_predicate$subexpression$1", "symbols": []},
    {"name": "like_predicate", "symbols": ["pre_bit_expr", "like_predicate$subexpression$1", "LIKE", "post_bit_expr"], "postprocess": 
        d => ({
          type: 'like',
          not: (d[1]||[]).length > 0,
          value: d[0],
          comparison: d[3]
        })
            },
    {"name": "pre_bit_expr", "symbols": ["bit_expr", "__"], "postprocess": d => d[0]},
    {"name": "pre_bit_expr", "symbols": [{"literal":"("}, "_", "bit_expr", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "post_bit_expr", "symbols": ["__", "bit_expr"], "postprocess": d => d[1]},
    {"name": "post_bit_expr", "symbols": [{"literal":"("}, "_", "bit_expr", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "bit_expr", "symbols": ["math"], "postprocess": d => d[0]},
    {"name": "math", "symbols": ["sum"], "postprocess": d => d[0]},
    {"name": "sum", "symbols": ["sum", "_", {"literal":"+"}, "_", "product"], "postprocess": opExprWs('+')},
    {"name": "sum", "symbols": ["sum", "_", {"literal":"-"}, "_", "product"], "postprocess": opExprWs('-')},
    {"name": "sum", "symbols": ["product"], "postprocess": d => d[0]},
    {"name": "product", "symbols": ["product", "_", {"literal":"/"}, "_", "simple_expr"], "postprocess": opExprWs('/')},
    {"name": "product", "symbols": ["product", "_", {"literal":"*"}, "_", "simple_expr"], "postprocess": opExprWs('*')},
    {"name": "product", "symbols": ["simple_expr"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["literal"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["identifier"], "postprocess": d => ({type: 'column', name: d[0].value})},
    {"name": "simple_expr", "symbols": ["distinct_expr"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["date_expr"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["now_expr"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["function_call"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["if_statement"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["ifnull_expr"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": [{"literal":"("}, "_", "expr_comma_list", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "simple_expr", "symbols": ["case_statement"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["cast_statement"], "postprocess": d => d[0]},
    {"name": "simple_expr", "symbols": ["identifier", {"literal":"."}, "identifier"], "postprocess": d => ({type: 'column', table: d[0].value, name: d[2].value})},
    {"name": "post_simple_expr", "symbols": ["__", "simple_expr"], "postprocess": d => d[1]},
    {"name": "post_simple_expr", "symbols": [{"literal":"("}, "_", "simple_expr", "_", {"literal":")"}], "postprocess": d => d[2]},
    {"name": "literal", "symbols": ["string"], "postprocess": d => d[0]},
    {"name": "literal", "symbols": ["decimal"], "postprocess": d => ({type: 'decimal', value: d[0]})},
    {"name": "literal", "symbols": ["NULL"], "postprocess": d => ({type: 'null'})},
    {"name": "literal", "symbols": ["TRUE"], "postprocess": d => ({type: 'true'})},
    {"name": "literal", "symbols": ["FALSE"], "postprocess": d => ({type: 'false'})},
    {"name": "expr_comma_list", "symbols": ["expr"], "postprocess": d => d[0]},
    {"name": "expr_comma_list", "symbols": ["expr_comma_list", "_", {"literal":","}, "_", "expr"], "postprocess":  d => {
        	if(d[0].type == 'expr_comma_list') {
        		return {
        			type: 'expr_comma_list',
        			exprs: d[0].exprs.concat(d[4])
        		};
        	}
        	return ({type:'expr_comma_list', exprs: [d[0],d[4]]});
        } },
    {"name": "if_statement", "symbols": ["IF", "_", {"literal":"("}, "_", "expr", "_", {"literal":","}, "_", "expr", "_", {"literal":","}, "_", "expr", "_", {"literal":")"}], "postprocess": 
        d => ({
          type: 'if',
          condition: d[4],
          then: d[8],
          'else': d[12]
        })
            },
    {"name": "case_statement$subexpression$1", "symbols": ["__"]},
    {"name": "case_statement$subexpression$1", "symbols": ["mid_expr"]},
    {"name": "case_statement$subexpression$2", "symbols": ["__", "ELSE", "__", "expr", "__"]},
    {"name": "case_statement$subexpression$2", "symbols": ["__"]},
    {"name": "case_statement", "symbols": ["CASE", "case_statement$subexpression$1", "when_statement_list", "case_statement$subexpression$2", "END"], "postprocess": 
        d => ({
          type: 'case',
          match: d[1][0],
          when_statements: d[2].statements,
          'else': (d[3]||[])[3]
        })
            },
    {"name": "when_statement_list", "symbols": ["when_statement"], "postprocess": d => ({statements: [d[0]]})},
    {"name": "when_statement_list", "symbols": ["when_statement_list", "__", "when_statement"], "postprocess":  d => ({
          statements: (d[0].statements||[]).concat([d[2]])
        })
          },
    {"name": "when_statement", "symbols": ["WHEN", "__", "expr", "__", "THEN", "__", "expr"], "postprocess": 
        d => ({
          type: 'when',
          condition: d[2],
          then: d[6]
        })
            },
    {"name": "ifnull_expr$subexpression$1", "symbols": [/[iI]/, /[fF]/, /[nN]/, /[uU]/, /[lL]/, /[lL]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "ifnull_expr", "symbols": ["ifnull_expr$subexpression$1", "_", {"literal":"("}, "_", {"literal":")"}], "postprocess": d => ({type:'ifnull'})},
    {"name": "ifnull_expr$subexpression$2", "symbols": [/[iI]/, /[fF]/, /[nN]/, /[uU]/, /[lL]/, /[lL]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "ifnull_expr", "symbols": ["ifnull_expr$subexpression$2", "_", {"literal":"("}, "_", "expr", "_", {"literal":","}, "_", "expr_comma_list", "_", {"literal":")"}], "postprocess": 
        d => ({
        	type: 'ifnull',
        	value: d[4],
        	rest: d[8]
        })
        		},
    {"name": "date_expr$subexpression$1", "symbols": [/[dD]/, /[aA]/, /[tT]/, /[eE]/, {"literal":"_"}, /[aA]/, /[dD]/, /[dD]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_expr", "symbols": ["date_expr$subexpression$1", "_", {"literal":"("}, "_", "expr", "_", {"literal":","}, "_", "interval_expr", "_", {"literal":")"}], "postprocess": dateExprFn('+',4,8)},
    {"name": "date_expr$subexpression$2", "symbols": [/[dD]/, /[aA]/, /[tT]/, /[eE]/, {"literal":"_"}, /[sS]/, /[uU]/, /[bB]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_expr", "symbols": ["date_expr$subexpression$2", "_", {"literal":"("}, "_", "expr", "_", {"literal":","}, "_", "interval_expr", "_", {"literal":")"}], "postprocess": dateExprFn('-',4,8)},
    {"name": "date_expr", "symbols": ["expr", "_", {"literal":"+"}, "_", "interval_expr"], "postprocess": dateExprFn('+',0,4)},
    {"name": "date_expr", "symbols": ["expr", "_", {"literal":"-"}, "_", "interval_expr"], "postprocess": dateExprFn('-',0,4)},
    {"name": "interval_expr", "symbols": ["INTERVAL", "__", "expr", "__", "date_unit"], "postprocess": 
        d => ({
            type: 'interval',
            value: d[2],
            unit: d[4]
          })
        },
    {"name": "now_expr$subexpression$1", "symbols": [/[nN]/, /[oO]/, /[wW]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "now_expr$string$1", "symbols": [{"literal":"("}, {"literal":")"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "now_expr", "symbols": ["now_expr$subexpression$1", "_", "now_expr$string$1"], "postprocess": d => ({type:'now'})},
    {"name": "now_expr$subexpression$2", "symbols": [/[nN]/, /[oO]/, /[wW]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "now_expr", "symbols": ["now_expr$subexpression$2", "_", {"literal":"("}, "_", {"literal":")"}], "postprocess": d => ({type:'now'})},
    {"name": "now_expr$subexpression$3", "symbols": [/[gG]/, /[eE]/, /[tT]/, /[dD]/, /[aA]/, /[tT]/, /[eE]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "now_expr$string$2", "symbols": [{"literal":"("}, {"literal":")"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "now_expr", "symbols": ["now_expr$subexpression$3", "_", "now_expr$string$2"], "postprocess": d => ({type:'now'})},
    {"name": "now_expr$subexpression$4", "symbols": [/[gG]/, /[eE]/, /[tT]/, /[dD]/, /[aA]/, /[tT]/, /[eE]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "now_expr", "symbols": ["now_expr$subexpression$4", "_", {"literal":"("}, "_", {"literal":")"}], "postprocess": d => ({type:'now'})},
    {"name": "cast_statement", "symbols": ["CAST", "_", {"literal":"("}, "_", "expr", "__", "AS", "__", "data_type", "_", {"literal":")"}], "postprocess": 
        d => ({
          type: 'cast',
          value: d[4],
          data_type: d[8]
        })
            },
    {"name": "CAST$subexpression$1", "symbols": [/[cC]/, /[aA]/, /[sS]/, /[tT]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "CAST", "symbols": ["CAST$subexpression$1"]},
    {"name": "AS$subexpression$1", "symbols": [/[aA]/, /[sS]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "AS", "symbols": ["AS$subexpression$1"]},
    {"name": "data_type$subexpression$1", "symbols": [/[dD]/, /[aA]/, /[tT]/, /[eE]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "data_type", "symbols": ["data_type$subexpression$1"], "postprocess": dataTypeFn},
    {"name": "data_type$subexpression$2", "symbols": [/[dD]/, /[aA]/, /[tT]/, /[eE]/, /[tT]/, /[iI]/, /[mM]/, /[eE]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "data_type", "symbols": ["data_type$subexpression$2"], "postprocess": dataTypeFn},
    {"name": "data_type$subexpression$3", "symbols": [/[tT]/, /[iI]/, /[mM]/, /[eE]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "data_type", "symbols": ["data_type$subexpression$3"], "postprocess": dataTypeFn},
    {"name": "data_type$subexpression$4", "symbols": [/[cC]/, /[hH]/, /[aA]/, /[rR]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "data_type", "symbols": ["data_type$subexpression$4"], "postprocess": dataTypeFn},
    {"name": "data_type$subexpression$5", "symbols": [/[sS]/, /[iI]/, /[gG]/, /[nN]/, /[eE]/, /[dD]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "data_type", "symbols": ["data_type$subexpression$5"], "postprocess": dataTypeFn},
    {"name": "data_type$subexpression$6", "symbols": [/[uU]/, /[nN]/, /[sS]/, /[iI]/, /[gG]/, /[nN]/, /[eE]/, /[dD]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "data_type", "symbols": ["data_type$subexpression$6"], "postprocess": dataTypeFn},
    {"name": "data_type$subexpression$7", "symbols": [/[bB]/, /[iI]/, /[nN]/, /[aA]/, /[rR]/, /[yY]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "data_type", "symbols": ["data_type$subexpression$7"], "postprocess": dataTypeFn},
    {"name": "distinct_expr", "symbols": ["DISTINCT", "__", "simple_expr"], "postprocess":  d => ({
        	type:'distinct',
        	operand: d[2]
        }) },
    {"name": "function_call", "symbols": ["function_identifier", "_", {"literal":"("}, "_", {"literal":"*"}, "_", {"literal":")"}], "postprocess":  d => ({
          type:'function_call',
          name: d[0],
          select_all: true
        }) },
    {"name": "function_call$string$1", "symbols": [{"literal":"("}, {"literal":")"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "function_call", "symbols": ["function_identifier", "_", "function_call$string$1"], "postprocess":  d => ({
          type: 'function_call',
          name: d[0],
          parameters: []
        })},
    {"name": "function_call", "symbols": ["function_identifier", "_", {"literal":"("}, "_", "expr_comma_list", "_", {"literal":")"}], "postprocess":  d => ({
          type: 'function_call',
          name: d[0],
          parameters: (d[4].exprs || [d[4]])
        })},
    {"name": "string", "symbols": ["dqstring"], "postprocess": d => ({type: 'string', string: d[0]})},
    {"name": "string", "symbols": ["sqstring"], "postprocess": d => ({type: 'string', string: d[0]})},
    {"name": "identifier", "symbols": ["btstring"], "postprocess": d => ({type: 'identifier', value:d[0]})},
    {"name": "identifier$ebnf$1$subexpression$1", "symbols": [/[^\]]/]},
    {"name": "identifier$ebnf$1$subexpression$1$string$1", "symbols": [{"literal":"\\"}, {"literal":"]"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "identifier$ebnf$1$subexpression$1", "symbols": ["identifier$ebnf$1$subexpression$1$string$1"]},
    {"name": "identifier$ebnf$1", "symbols": ["identifier$ebnf$1$subexpression$1"]},
    {"name": "identifier$ebnf$1$subexpression$2", "symbols": [/[^\]]/]},
    {"name": "identifier$ebnf$1$subexpression$2$string$1", "symbols": [{"literal":"\\"}, {"literal":"]"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "identifier$ebnf$1$subexpression$2", "symbols": ["identifier$ebnf$1$subexpression$2$string$1"]},
    {"name": "identifier$ebnf$1", "symbols": ["identifier$ebnf$1", "identifier$ebnf$1$subexpression$2"], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "identifier", "symbols": [{"literal":"["}, "identifier$ebnf$1", {"literal":"]"}], "postprocess": d => ({type: 'identifier', value: d[1].map(x => x[0]).join('')})},
    {"name": "identifier$ebnf$2", "symbols": []},
    {"name": "identifier$ebnf$2", "symbols": ["identifier$ebnf$2", /[a-zA-Z0-9_]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "identifier", "symbols": [/[a-zA-Z_]/, "identifier$ebnf$2"], "postprocess":  (d,l,reject) => {
          const value = d[0] + d[1].join('');
          if(['NULL','TRUE','FALSE'].indexOf(value.toUpperCase()) != -1) return reject;
          return {type: 'identifier', value: value};
        } },
    {"name": "function_identifier", "symbols": ["btstring"], "postprocess": d => ({value:d[0]})},
    {"name": "function_identifier$ebnf$1", "symbols": []},
    {"name": "function_identifier$ebnf$1", "symbols": ["function_identifier$ebnf$1", /[a-zA-Z0-9_]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "function_identifier", "symbols": [/[a-zA-Z_]/, "function_identifier$ebnf$1"], "postprocess":  (d,l,reject) => {
            const value = d[0] + d[1].join('');
        if(value.toLowerCase() == 'if') return reject;
        if(value.toLowerCase() == 'ifnull') return reject;
            //if(reserved.indexOf(value.toUpperCase()) != -1 && valid_function_identifiers.indexOf(value.toUpperCase()) == -1) return reject;
        // if(functions.indexOf(value.toUpperCase()) == -1) return reject;
            return {value: value};
          } },
    {"name": "DISTINCT$subexpression$1", "symbols": [/[dD]/, /[iI]/, /[sS]/, /[tT]/, /[iI]/, /[nN]/, /[cC]/, /[tT]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "DISTINCT", "symbols": ["DISTINCT$subexpression$1"]},
    {"name": "INTERVAL$subexpression$1", "symbols": [/[iI]/, /[nN]/, /[tT]/, /[eE]/, /[rR]/, /[vV]/, /[aA]/, /[lL]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "INTERVAL", "symbols": ["INTERVAL$subexpression$1"]},
    {"name": "NULL", "symbols": [/[nN]/, /[uU]/, /[lL]/, /[lL]/]},
    {"name": "AND", "symbols": [/[aA]/, /[nN]/, /[dD]/]},
    {"name": "OR", "symbols": [/[oO]/, /[rR]/]},
    {"name": "XOR", "symbols": [/[xX]/, /[oO]/, /[rR]/]},
    {"name": "NOT", "symbols": [/[nN]/, /[oO]/, /[tT]/]},
    {"name": "TRUE", "symbols": [/[tT]/, /[rR]/, /[uU]/, /[eE]/]},
    {"name": "FALSE", "symbols": [/[fF]/, /[aA]/, /[lL]/, /[sS]/, /[eE]/]},
    {"name": "IS", "symbols": [/[iI]/, /[sS]/]},
    {"name": "IN", "symbols": [/[iI]/, /[nN]/]},
    {"name": "BETWEEN", "symbols": [/[bB]/, /[eE]/, /[tT]/, /[wW]/, /[eE]/, /[eE]/, /[nN]/]},
    {"name": "LIKE", "symbols": [/[lL]/, /[iI]/, /[kK]/, /[eE]/]},
    {"name": "IF", "symbols": [/[iI]/, /[fF]/]},
    {"name": "ELSE", "symbols": [/[eE]/, /[lL]/, /[sS]/, /[eE]/]},
    {"name": "CASE", "symbols": [/[cC]/, /[aA]/, /[sS]/, /[eE]/]},
    {"name": "WHEN", "symbols": [/[wW]/, /[hH]/, /[eE]/, /[nN]/]},
    {"name": "THEN", "symbols": [/[tT]/, /[hH]/, /[eE]/, /[nN]/]},
    {"name": "END", "symbols": [/[eE]/, /[nN]/, /[dD]/]},
    {"name": "date_unit$subexpression$1", "symbols": [/[mM]/, /[iI]/, /[cC]/, /[rR]/, /[oO]/, /[sS]/, /[eE]/, /[cC]/, /[oO]/, /[nN]/, /[dD]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$1"]},
    {"name": "date_unit$subexpression$2", "symbols": [/[sS]/, /[eE]/, /[cC]/, /[oO]/, /[nN]/, /[dD]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$2"]},
    {"name": "date_unit$subexpression$3", "symbols": [/[mM]/, /[iI]/, /[nN]/, /[uU]/, /[tT]/, /[eE]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$3"]},
    {"name": "date_unit$subexpression$4", "symbols": [/[hH]/, /[oO]/, /[uU]/, /[rR]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$4"]},
    {"name": "date_unit$subexpression$5", "symbols": [/[dD]/, /[aA]/, /[yY]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$5"]},
    {"name": "date_unit$subexpression$6", "symbols": [/[wW]/, /[eE]/, /[eE]/, /[kK]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$6"]},
    {"name": "date_unit$subexpression$7", "symbols": [/[mM]/, /[oO]/, /[nN]/, /[tT]/, /[hH]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$7"]},
    {"name": "date_unit$subexpression$8", "symbols": [/[qQ]/, /[uU]/, /[aA]/, /[rR]/, /[tT]/, /[eE]/, /[rR]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$8"]},
    {"name": "date_unit$subexpression$9", "symbols": [/[yY]/, /[eE]/, /[aA]/, /[rR]/], "postprocess": function(d) {return d.join(""); }},
    {"name": "date_unit", "symbols": ["date_unit$subexpression$9"]},
    {"name": "dqstring$ebnf$1", "symbols": []},
    {"name": "dqstring$ebnf$1", "symbols": ["dqstring$ebnf$1", "dstrchar"], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "dqstring", "symbols": [{"literal":"\""}, "dqstring$ebnf$1", {"literal":"\""}], "postprocess": function(d) {return d[1].join(""); }},
    {"name": "sqstring$ebnf$1", "symbols": []},
    {"name": "sqstring$ebnf$1", "symbols": ["sqstring$ebnf$1", "sstrchar"], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "sqstring", "symbols": [{"literal":"'"}, "sqstring$ebnf$1", {"literal":"'"}], "postprocess": function(d) {return d[1].join(""); }},
    {"name": "btstring$ebnf$1", "symbols": []},
    {"name": "btstring$ebnf$1", "symbols": ["btstring$ebnf$1", /[^`]/], "postprocess": function arrpush(d) {return d[0].concat([d[1]]);}},
    {"name": "btstring", "symbols": [{"literal":"`"}, "btstring$ebnf$1", {"literal":"`"}], "postprocess": function(d) {return d[1].join(""); }},
    {"name": "dstrchar", "symbols": [/[^\\"\n]/], "postprocess": id},
    {"name": "dstrchar", "symbols": [{"literal":"\\"}, "strescape"], "postprocess": 
        function(d) {
          return JSON.parse("\""+d.join("")+"\"");
        }
        },
    {"name": "sstrchar", "symbols": [/[^\\'\n]/], "postprocess": id},
    {"name": "sstrchar", "symbols": [{"literal":"\\"}, "strescape"], "postprocess": 
        function(d) {
          return JSON.parse("\""+d.join("")+"\"");
        } },
    {"name": "sstrchar$string$1", "symbols": [{"literal":"\\"}, {"literal":"'"}], "postprocess": function joiner(d) {return d.join('');}},
    {"name": "sstrchar", "symbols": ["sstrchar$string$1"], "postprocess": function(d) {return "'"; }},
    {"name": "strescape", "symbols": [/["\\/bfnrt]/], "postprocess": id},
    {"name": "strescape", "symbols": [{"literal":"u"}, /[a-fA-F0-9]/, /[a-fA-F0-9]/, /[a-fA-F0-9]/, /[a-fA-F0-9]/], "postprocess": 
        function(d) {
            return d.join("");
        }
        }
]
  , ParserStart: "main"
}
if (typeof module !== 'undefined'&& typeof module.exports !== 'undefined') {
   module.exports = grammar;
} else {
   window.grammar = grammar;
}
})();
