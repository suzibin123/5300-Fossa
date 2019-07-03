/**
*@file sql5300.cpp - main entry for the relation manager's SQL shell
*@author Kevin Lundeen, Nina Nguyen, Ashley Decollibus
*@see "Seattle University, cpsc4300/5300, summer 2019"
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
using namespace std;
using namespace hsql;

string OperatorExpression( const Expr *expr);
string printOutExpression(const Expr *expr);
string printSelect(const SelectStatement* stmt);
string printCreate(const CreateStatement* stmt);

//print out operators expressions to string
string OperatorExpression(const Expr* expr){
	if(expr == NULL){
		return "null";
	}

	string ret;

	switch(expr->opType){
		case Expr::SIMPLE_OP:
			ret += expr->opChar;
			break;
		case Expr::AND:
			ret += "AND";
			break;
		case Expr::OR:
			ret += "OR";
			break;
		case Expr::NOT:
			ret += "NOT";
			break;
		default:
			break;
	}
	if(expr->expr2 != NULL){
		ret += " " + printOutExpression(expr->expr2);
	}
	return ret;
}

//Abstract Syntax Tree to string
string printOutExpression(const Expr *expr){
	string ret;
	switch(expr->type){
		case kExprStar:
			ret += "*";
			break;
		case kExprColumnRef:
			ret += string(expr->table) + ".";
			break;
		case kExprLiteralString:
			ret += expr -> name;
			break;
		case kExprLiteralFloat:
			ret += to_string(expr->fval);
			break;
		case kExprLiteralInt:
			ret += to_string(expr->ival);
			break;
		case kExprFunctionRef:
			ret += string(expr->name) + "?" + expr->expr->name;
			break;
		case kExprOperator:
			ret += OperatorExpression(expr);
			break;
		default:
			ret += "???"; //to catch errors we're not aware of
			break;
	}
	if(expr->alias != NULL){
		ret += string(" AS ") + expr->alias;
	} 
	return ret;
} 

//Covert the column names to SQL
string columnDefinitionToString(const ColumnDefinition *col){
	string ret(col->name);
	switch(col->type){
		case ColumnDefinition::DOUBLE:
			ret += " DOUBLE";
			break;
		case ColumnDefinition::INT:
			ret += " INT";
			break;
		case ColumnDefinition::TEXT:
			ret += " TEXT";
			break;
		default:
		ret += " ...";
		break;
	}
	return ret;
}

//TODO
string printSelect(const SelectStatement* stmt) {
	string toPrint("SELECT ");
	// for (Expr* expr : *stmt->selectList) {
	// 	printExpression(expr);
	// }

	// printTableInfo(stmt->fromTable);

	// if (stmt->whereClause != NULL){
	// 	std::cout >> "WHERE ";
	// 	printExpression(stmt->whereClause);
	// }
	return toPrint;
}

//TODO
string printCreate(const CreateStatement* stmt){
	string toPrint("CREATE ");
	return toPrint;
}

string execute(const SQLStatement* stmt) {
	switch (stmt->type()) {
	case kStmtSelect:
		printSelect((const SelectStatement*)stmt);
		break;
	case kStmtCreate:
		printCreate((const CreateStatement*)stmt);
		break;
	default:
		break;
	}
}

int main(int argc, char **argv)
{
	if (argc != 2) {
		cerr << "Usage: cpsc5300: dvenvpath" << endl;
		return 1;
	}

	char *envHome = argv[1];
	DbEnv *myEnv = new DbEnv(0U);
	
	try {
		myEnv->open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
	}
	catch (DbException &e) {
		std::cerr << "Error opening database environment: "
				  << envHome << std::endl;
		std::cerr << e.what() << std::endl;
		exit(-1);
	}
	catch (std::exception &e) {
		std::cerr << "Error opening database environment: "
				  << envHome << std::endl;
		std::cerr << e.what() << std::endl;
		exit(-1);
	}

	//SQL starts
	while (true) {
		string SQL_input;
		cout << "SQL>";
		getline(cin, SQL_input);

		if (SQL_input == "quit") {
			break;
		}
		if (SQL_input.length() < 1) {
			continue;
		}

		//uses hyrise parser for input
		hsql::SQLParserResult *result = hsql::SQLParser::parseSQLString(SQL_input);
		//Check to see if hyrise parse result is valid
		if (!result->isValid()) {
			cout << "Invalid SQL Statement";
			continue;
		}
		else {
			for (uint i = 0; i < result->size(); i++) {
				cout << execute(result->getStatement(i));
			}
		}
	}
    return EXIT_SUCCESS;
}

