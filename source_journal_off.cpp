//#include <stdio.h>
//#include <stdlib.h>
#include <string>
#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <sqlite3.h>

/*
compile with  g++ -o temp source.cpp -Wall -lsqlite3
*/

using namespace std;

void Tokenize (const string& str, vector<string>& tokens, const string& delimiters){
	//skip any opening delims
	string::size_type lastPos = str.find_first_not_of(delimiters,0);
	//find the first non-delim
	string::size_type pos = str.find_first_of(delimiters,lastPos);

	while(string::npos != pos || string::npos != lastPos ){
		tokens.push_back(str.substr(lastPos,pos-lastPos));
		lastPos=str.find_first_not_of(delimiters,pos);
		pos=str.find_first_of(delimiters,lastPos);
	}
}

int main(){
	int i=0;
	int rc;
	int x;
	sqlite3 *db;
	char *db_err;
	sqlite3_stmt *pStmt;
	sqlite3_stmt *pStmt2;
	rc = sqlite3_open("profile.db",&db);
	string sql;
	string sql2;
	if ( rc ){
		std::cout << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
		sqlite3_close(db);
		return 1;
	}
	sqlite3_exec(db, "PRAGMA syncronous=OFF;PRAGMA count_changes=OFF;PRAGMA journal_mode=OFF;drop table if exists profile;create table profile ( columnid varchar(50), field_val varchar(255), freq int);BEGIN TRANSACTION;",NULL, 0, &db_err);
	sqlite3_exec(db, "create unique index x1 on profile(columnid,field_val);",NULL,0,&db_err);
	sql = "insert or ignore into profile values(?,?,0);";
	sql2= "update profile set freq = freq +1 where columnid=? and field_val=?;";
	if ( sqlite3_prepare_v2(db,sql.c_str(),sql.size(),&pStmt,NULL) != SQLITE_OK){
		std::cout << "problem" << sqlite3_errmsg(db) <<std::endl;
		return 1;
	}

	if ( sqlite3_prepare_v2(db,sql2.c_str(),sql2.size(),&pStmt2,NULL) != SQLITE_OK){
		std::cout << "problem" << sqlite3_errmsg(db) <<std::endl;
		return 1;
	}

	ifstream infile("expressor_100k_perf.txt");
	vector<string> headerRow ;
	while ( ! infile.eof() ){
		vector<string> tokens ;
		vector<string>::iterator tokenIterator;
	
		string line ;
		getline(infile,line);
		Tokenize(line,tokens,"|");		
		if ( i == 0 ) {
			tokenIterator=tokens.begin();
			headerRow.assign(tokenIterator,tokens.end());

		}else{	
			for( x = 0 ; x < (int) tokens.size() ; x++){
				sqlite3_bind_text(pStmt,1, headerRow[x].c_str() , -1,SQLITE_STATIC);
		
				sqlite3_bind_text(pStmt,2,tokens[x].c_str(),-1,SQLITE_STATIC);
				sqlite3_bind_text(pStmt2,1, headerRow[x].c_str() , -1,SQLITE_STATIC);
				sqlite3_bind_text(pStmt2,2,tokens[x].c_str(),-1,SQLITE_STATIC);
				if ( sqlite3_step(pStmt) != SQLITE_DONE){
							std::cout << "problem inserting row!" << sqlite3_errmsg(db) <<std::endl;
							return 1;
				}
				sqlite3_reset(pStmt);
				if ( sqlite3_step(pStmt2) != SQLITE_DONE){
							std::cout << "problem inserting row!" << sqlite3_errmsg(db) <<std::endl;
				}
				sqlite3_reset(pStmt2);
			}
		}

		i++;
		if ( i%500 == 0 ){
			std::cout << i << " rows processed..." << std::endl;
		}
	}
	sqlite3_exec(db, "END TRANSACTION;",NULL, 0, &db_err);
	std::cout << "Complete.  " << i << " rows processed." << std::endl;
	return 0;
}

