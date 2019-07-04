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
string printTableInfo(const TableRef* table);

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
/**
*Helper function to spit out arithmetic and conditionals in SQL
**/
=======
//print out operators expressions to string
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//print out operators expressions to string
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//print out operators expressions to string
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
string OperatorExpression(const Expr* expr){
	if(expr == NULL){
		return "null";
	}

	string ret;

	ret += printOutExpression(expr->expr) + " ";

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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
/**
*Helper function to convert Abstract Syntax Tree to string
**/
=======
//Abstract Syntax Tree to string
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//Abstract Syntax Tree to string
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//Abstract Syntax Tree to string
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
string printOutExpression(const Expr *expr){
	string ret;
	switch(expr->type){
		case kExprStar:
			ret += "*";
			break;
		case kExprColumnRef:
			if(expr->table != NULL){
				ret += string(expr->table) + ".";
			}
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
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
			ret += "Invalid expression type"; //to catch errors we're not aware of
=======
			ret += "???"; //to catch errors we're not aware of
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
			ret += "???"; //to catch errors we're not aware of
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
			ret += "???"; //to catch errors we're not aware of
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
			break;
	}
	if(expr->alias != NULL){
		ret += string(" AS ") + expr->alias;
	} 
	return ret;
} 

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
/**
*Helper function to spit out variable type in SQL
**/
=======
//Covert the column names to SQL
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//Covert the column names to SQL
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//Covert the column names to SQL
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
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

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
/**
*Helper function to spit out Join operators, table names and alias
**/
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
string printTableInfo(const TableRef* table){
	string ret;
	switch (table->type){
	case kTableName: 
		ret +=(table->name);
		if( table->alias != NULL){
			ret += string(" AS ") + table->alias;
		}
		break; 
	case kTableSelect: 
		printSelect(table->select);
		break; 
	case kTableJoin: 
		ret += printTableInfo(table->join->left);
		switch(table->join->type){
			case kJoinLeft:
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
				ret += " LEFT JOIN ";
				break;
			case kJoinRight:
				ret += " RIGHT JOIN "; 
				break;
			case kJoinInner:
				ret += " JOIN "; 
				break; 
			case kJoinOuter:
				ret += " OUTER JOIN ";
				break;
			case kJoinCross:
				ret += " CROSS JOIN ";
				break;
			default:
				cout << "Not yet implemented" << endl;
				break;
			} 
			ret += printTableInfo(table->join->right);
			if (table->join->condition != NULL){
				ret += " ON " + OperatorExpression(table->join->condition);
			}
		break;	
	case kTableCrossProduct:
		{
		bool comma = false;
		for(TableRef* tbl : *table->list) {
			if(comma){
				ret += ", ";
			}
			ret += printTableInfo(tbl); 
			comma = true;
		}
		break;
		}
	default:
		cout << "Not yet implemented" << endl;
		break;
=======
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
				ret += " Left join ";
				printTableInfo(table->join->left);
				break;
			case kJoinRight:
				ret += " Right join "; 
				printTableInfo(table->join->right); 
				break;
			case kJoinInner:
				ret += " Join "; 
				printOutExpression(table->join->condition); 
				break; 
			default:
				cout << "Not yet implemented" << endl;
				break;
		} 
		ret += printTableInfo(table->join->right);
		if (table->join->condition != NULL){
			ret += " ON " + OperatorExpression(table->join->condition);
		}
		break;	
	case kTableCrossProduct:
		for(TableRef* tbl : *table->list) printTableInfo(tbl); 
		break;
	default:
		cout << "Not yet implemented" << endl;
		break;
	 
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
	}
	return ret;
}


<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
/**
*Helper function to print Select SQL Statement
**/
=======
//TODO
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//TODO
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
//TODO
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
string printSelect(const SelectStatement* stmt) {
	string ret = "SELECT ";
	bool comma = false;
	for (Expr* expr : *stmt->selectList) {
		if(comma){
			ret += ", ";
		}
		ret += printOutExpression(expr);
		comma = true;
	}

	ret += " FROM " + printTableInfo(stmt->fromTable);

	if (stmt->whereClause != NULL){
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
		ret += " WHERE " + printOutExpression(stmt->whereClause);
	}
	if (stmt->order != NULL){
		std::cout << "ORDER BY ";
		ret += printOutExpression(stmt->order->at(0)->expr);
		if(stmt->order->at(0)->type == kOrderAsc){
			std::cout << "ASCENDING ";
		} else {
			std::cout << "DESCENDING ";
		}
=======
		std::cout << "WHERE " << endl;
		ret += printOutExpression(stmt->whereClause);
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
		std::cout << "WHERE " << endl;
		ret += printOutExpression(stmt->whereClause);
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
		std::cout << "WHERE " << endl;
		ret += printOutExpression(stmt->whereClause);
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
	}
	return ret;
}

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
/**
*Heler function for CREATE SQL statement
**/
string printCreate(const CreateStatement* stmt){
	string ret;
	ret += "CREATE TABLE ";
	if(stmt->type != CreateStatement::kTable){
		return ret + "Table is invalid";
=======
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
//TODO
string printCreate(const CreateStatement* stmt){
	string ret("CREATE TABLE ");
	if(stmt->type != CreateStatement::kTable){
		return ret + "...";
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
	}
	if(stmt->ifNotExists){
		ret += "IF NOT EXIST ";
	}
	ret += string(stmt->tableName) + " (";
	bool comma = false;
	for (ColumnDefinition *col : *stmt->columns){
		if(comma){
			ret += ", ";
		}
		ret += columnDefinitionToString(col);
		comma = true;
	}
	ret += ")";
	return ret;
}

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
/**
*Helper function to handle different SQL queries
**/
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
string execute(const SQLStatement* stmt) {
	switch (stmt->type()) {
	case kStmtSelect:
		return printSelect((const SelectStatement*)stmt);
		break;
	case kStmtCreate:
		return printCreate((const CreateStatement*)stmt);
		break;
	default:
		return "Not implemented yet";
		break;
	}
}

int main(int argc, char **argv)
{
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	//Check for Command line paramenters, throw errors if there's more than two
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
	if (argc != 2) {
		cerr << "Usage: cpsc5300: dvenvpath" << endl;
		return 1;
	}

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	//Store cmd line arg directory path
	char *envHome = argv[1];
	DbEnv *myEnv = new DbEnv(0U);
	
	//create database env if one doesn't exist
=======
	char *envHome = argv[1];
	DbEnv *myEnv = new DbEnv(0U);
	
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
	char *envHome = argv[1];
	DbEnv *myEnv = new DbEnv(0U);
	
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
=======
	char *envHome = argv[1];
	DbEnv *myEnv = new DbEnv(0U);
	
>>>>>>> fb4690c44891367fd067d435a29731fd9b455943
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
				cout << execute(result->getStatement(i)) << endl;
			}
		}
	}
    return EXIT_SUCCESS;
}

