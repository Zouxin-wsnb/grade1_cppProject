#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <sstream>
#include <stack>
#include <iterator>
#include <unordered_map>
#include <algorithm>
using namespace std;

struct Column {  //将列名和类型分开
    string name;
    string type;
};

class Database {
public:
    string name;
    unordered_map<string, vector<vector<string>>> tables;  // 存储表的数据
    unordered_map<string, vector<Column>> tableColumns;  // 存储表的列信息

    Database() = default;
    Database(const string& dbName) : name(dbName) {}
};

class MiniDB {
public:

    unordered_map<string, Database> databases;
    Database* currentDatabase = nullptr;
    bool isprint=false;

void create_database(const string& dbName)
{
    if (databases.find(dbName) != databases.end()) {
        cerr << "Database " << dbName << " already exists" << endl;
        return;
    }
    databases[dbName] = Database(dbName);
    save_database(databases[dbName]);
}

void use_database(const string dbName)
{
    string dbFileName = dbName + ".db";
    ifstream file(dbFileName);
    if (!file.is_open()) {
        cerr << "Database " << dbName << " does not exist" << endl;
        return;
    }
    file.close();

    load_database(dbFileName);
}

void create_table(const string& tableName, const vector<string>& columns) {
    if(!currentDatabase) {
        cerr << "No database selected" << endl;
        return;
    }
    
    if (currentDatabase->tables.find(tableName) != currentDatabase->tables.end()) {
        cerr << "Table " << tableName << " already exists" << endl;
        return;
    }
    
    vector<Column> tableColumns;
    vector<string> columnNames;
    
    for(const auto& col : columns) {
        istringstream iss(col);
        string name, type;
        iss >> name >> type;
        tableColumns.push_back({name, type});
        columnNames.push_back(name);
    }
    
    currentDatabase->tables[tableName] = {columnNames};  // 只存储列名
    currentDatabase->tableColumns[tableName] = tableColumns;  // 存储列名和类型
    save_database(*currentDatabase);
}

void drop_table(const string& tableName)
{
    if(currentDatabase)
    {
        if (currentDatabase->tables.find(tableName) == currentDatabase->tables.end()) {
            cerr << "Table " << tableName << " does not exist" << endl;
            return;
        }
        currentDatabase->tables.erase(tableName);
        save_database(*currentDatabase);
    }
    else
    {
        cerr<<"No database selected"<<endl;
    
    }
}

void insert_into_table(const string& tableName, const vector<string>& values)
{
    if(currentDatabase)
    {
        if(currentDatabase->tables.find(tableName)!=currentDatabase->tables.end())
        {
            vector<string> cleanValues;
        for(size_t i = 0; i < values.size(); ++i) {
            string cleanValue = values[i];
            // 根据列类型处理数据
            string colType = currentDatabase->tableColumns[tableName][i].type;
            
           // 去除逗号
            while(!cleanValue.empty() && (cleanValue.back() == ',' || cleanValue.back() == ' ')) {
                cleanValue.pop_back();
            }

            // 如果是TEXT类型，确保字符串被单引号包裹
            if(colType == "TEXT") {
                if(cleanValue.front() != '\'' || cleanValue.back() != '\'') {
                    cerr << "Invalid TEXT value format: " << cleanValue << endl;
                    return;
                }
            }
            
            cleanValues.push_back(cleanValue);
        }
            currentDatabase->tables[tableName].push_back(cleanValues);
            save_database(*currentDatabase);
        }
        else
        {
            cerr<<"Table "<<tableName<<" does not exist"<<endl;
        }
    }
    else
    {
        cerr<<"No database selected"<<endl;
    }
}

void select_to_file(const string& tableName, const vector<string>& columnNames, const vector<string>& conditions, const string& outputFile)
{
    if (!currentDatabase) {
        cerr << "No database selected" << endl;
        return;
    }

    if (currentDatabase->tables.find(tableName) == currentDatabase->tables.end()) {
        cerr << "Table " << tableName << " does not exist" << endl;
        return;
    }

    // 使用追加模式打开文件
    ofstream file(outputFile, ios::app);
    if (!file.is_open()) {
        cerr << "Unable to open file: " << outputFile << endl;
        return;
    }

    if(isprint)
    {
        file<<"---"<<endl;
    }
    else
    {
        isprint=true;
    }

    const auto& table = currentDatabase->tables[tableName];
    const auto& header = table.front();

    for(const auto& col:columnNames)
    {
        file<<col;
        if(col!=columnNames.back())
        {
            file<<",";
        }
    }
    file<<endl;

    vector<size_t> colIndices;
    if (columnNames.size() == 1 && columnNames[0] == "*") {
        for (size_t i = 0; i < header.size(); ++i) {
            colIndices.push_back(i);
        }
    } else {
        for (const auto& colName : columnNames) {
            auto it = find(header.begin(), header.end(), colName);
            if (it != header.end()) {
                colIndices.push_back(distance(header.begin(), it));
            } else {
                cerr << "Column " << colName << " does not exist in table " << tableName << endl;
                return;
            }
        }
    }
        for (size_t i = 1; i < table.size(); ++i) {
        const auto& row = table[i];
        if (conditions.empty() || evaluateConditions(row, header, conditions, tableName)) {
            // 输出满足条件的行
            for (size_t j = 0; j < colIndices.size(); ++j) {
                file << row[colIndices[j]];
                if (j < colIndices.size() - 1) file << ",";
            }
            file << endl;
        }
    }
    file.close();
}

void inner_join_file(const string& table1, const string& table2, const vector<string>& columnNames, const vector<string>& conditions, const string& outputFile)
{
    if(!currentDatabase) {
        cerr << "No database selected" << endl;
        return;
    }
    
    if(currentDatabase->tables.find(table1) == currentDatabase->tables.end() || 
       currentDatabase->tables.find(table2) == currentDatabase->tables.end()) {
        cerr << "Table does not exist" << endl;
        return;
    }

    // 使用追加模式
    ofstream file(outputFile, ios::app);
    if(!file.is_open()) {
        cerr << "Unable to open file: " << outputFile << endl;
        return;
    }

    if(isprint)
    {
        file<<"---"<<endl;
    }
    else
    {
        isprint=true;
    }

    const auto& res_table = currentDatabase->tables[table1];
    const auto& tag_table = currentDatabase->tables[table2];
    const auto& res_header = res_table.front();
    const auto& tag_header = tag_table.front();
    
    // 列名
    vector<pair<string,string>> join_columns;
    vector<pair<string,string>> join_conditions;
    vector<string> where_conditions;
    pair<string,string> table_names;
    //分离列名
    vector<string> join= vector<string>(conditions.begin(), conditions.begin() + 2);
    vector<string> where= vector<string>(conditions.begin() + 2, conditions.end());
 
    for(const auto& column : columnNames) {
        size_t dot_pos = column.find('.');
        join_columns.push_back(make_pair(
            column.substr(0, dot_pos),
            column.substr(dot_pos + 1)
        ));
    }

    for(const auto& condition : join) {
        size_t dot_pos = condition.find('.');
        join_conditions.push_back(make_pair(
            condition.substr(0, dot_pos),
            condition.substr(dot_pos + 1)
        ));
    }


    for(const auto& condition : where) {
        if(condition.find('.')&&table_names.first.empty())
        {
            size_t dot_pos = condition.find('.');
            table_names.first=condition.substr(0,dot_pos);
            where_conditions.push_back(condition.substr(dot_pos+1));
        }
        else if(where.size()>3&&condition==where[3])
        {
            size_t dot_pos = condition.find('.');
            table_names.second=condition.substr(0,dot_pos);
            where_conditions.push_back(condition.substr(dot_pos+1));
        }
        else
        {
            where_conditions.push_back(condition);
        }
    }

    // 写入列名
    for(size_t i = 0; i < join_columns.size(); ++i) {
        file <<join_columns[i].first<<"." <<join_columns[i].second;
        if(i < join_columns.size() - 1) {
            file << ",";
        }
    }
    file << endl;

    // 连接
    for(size_t i = 1; i < res_table.size(); i++) {
        for(size_t j = 1; j < tag_table.size(); j++) {
            bool match = true;
            
            // 检查连接条件
            auto it1 = find(res_header.begin(), res_header.end(), join_conditions[0].second);
            auto it2 = find(tag_header.begin(), tag_header.end(), join_conditions[1].second);

            if(it1 != res_header.end() && it2 != tag_header.end()) {
                size_t index1 = distance(res_header.begin(), it1);
                size_t index2 = distance(tag_header.begin(), it2);
    
                if(res_table[i][index1] != tag_table[j][index2]) {
                    match = false;
                }
            }

            //检查WHERE条件
            if(!where_conditions.empty())
            {
                if(where_conditions.size()>3)
                {
                    string and_or=where_conditions[3];
                    const auto& current_table1=currentDatabase->tables[table_names.first];
                    const auto& current_table2=currentDatabase->tables[table_names.second];
                    if(and_or=="AND")
                    {
                        match=match&&evaluateSingleCondition(current_table1[i],current_table1[0],where_conditions[0],where_conditions[1],where_conditions[2],table_names.first)&&evaluateSingleCondition(current_table2[j],current_table2[0],where_conditions[4],where_conditions[5],where_conditions[6],table_names.second);
                    }
                    else if(and_or=="OR")
                    {
                        match=match&&(evaluateSingleCondition(current_table1[i],current_table1[0],where_conditions[0],where_conditions[1],where_conditions[2],table_names.first)||evaluateSingleCondition(current_table2[j],current_table2[0],where_conditions[4],where_conditions[5],where_conditions[6],table_names.second));
                    }
                }
                else
                {
                    const auto& current_table=currentDatabase->tables[table_names.first];
                    match=match&&evaluateSingleCondition(current_table[i],current_table[0],where_conditions[0],where_conditions[1],where_conditions[2],table_names.first);
                }
            }

            // 输出匹配的行
            if(match) {
                for(size_t k = 0; k < join_columns.size(); ++k) {
                    const auto& col = join_columns[k];
                    if(col.first == table1) {
                        auto it = find(res_header.begin(), res_header.end(), col.second);
                        if(it != res_header.end()) {
                            size_t index = distance(res_header.begin(), it);
                            file << res_table[i][index];
                        }
                    } else {
                        auto it = find(tag_header.begin(), tag_header.end(), col.second);
                        if(it != tag_header.end()) {
                            size_t index = distance(tag_header.begin(), it);
                            file << tag_table[j][index];
                        }
                    }
                    
                    if(k < join_columns.size() - 1) {
                        file << ",";
                    }
                }
                file << endl;
            }
        }
    }
    
    file.close();
}

void update_table(const string& tableName, const vector<pair<string, string>>& updates, vector<string>& conditions) {
    if(!currentDatabase || currentDatabase->tables.find(tableName) == currentDatabase->tables.end()) {
        return;
    }

    auto& table = currentDatabase->tables[tableName];
    const auto& header = table.front();

    for(size_t i = 1; i < table.size(); i++) {
        bool match = true;
        if(!conditions.empty()) {
            const auto& row = table[i];
            if(!evaluateConditions(row, header, conditions, tableName)) {
                match = false;
            }
        }
        if(match) {
            for(const auto& update : updates) {
                auto it = find(header.begin(), header.end(), update.first);
                if(it != header.end()) {
                    size_t index = distance(header.begin(), it);
                    string colType = currentDatabase->tableColumns[tableName][index].type;
                    
                    // 处理表达式
                    string newValue = update.second;
                    if(colType == "INTEGER" || colType == "FLOAT") {
                        // 替换表达式中的列名为实际值
                        string expr = newValue;
                        for(const auto& col : currentDatabase->tableColumns[tableName]) {
                            size_t pos = 0;
                            while((pos = expr.find(col.name, pos)) != string::npos) {
                                auto valIt = find(header.begin(), header.end(), col.name);
                                if(valIt != header.end()) {
                                    size_t valIndex = distance(header.begin(), valIt);
                                    expr.replace(pos, col.name.length(), table[i][valIndex]);
                                }
                                pos += col.name.length();
                            }
                        }
                        
                        // 计算表达式结果
                        try {
                            istringstream iss(expr);
                            float result = evaluateExpression(iss);
                            if(colType == "INTEGER") {
                                table[i][index] = to_string(static_cast<int>(result));
                            } else {
                                table[i][index] = to_string(result);
                            }
                        } catch(...) {
                            cerr << "Invalid expression: " << expr << endl;
                        }
                    } else {
                        table[i][index] = newValue;
                    }
                }
            }
        }
    }
    save_database(*currentDatabase);
}

void deleteFromTable(const string& tableName, vector<string>& conditions)
{
    if(currentDatabase && currentDatabase->tables.find(tableName)!=currentDatabase->tables.end())
    {
        auto& table = currentDatabase->tables[tableName];
        if(conditions.empty())
        {
            table.erase(table.begin()+1,table.end()); 
        }
        else
        {
            const auto& header = table.front();
            for(size_t i = 1; i < table.size();) // 移除循环变量的递增
            {
                bool match = false;  
                auto row = table[i];
                if(conditions.empty()||evaluateConditions(row, header, conditions, tableName)) {
                    match = true;
                }
                if(match) {
                    table.erase(table.begin() + i);
                } else {
                    i++; 
                }
            }
        }
        save_database(*currentDatabase);
    }
}

void save_database(const Database& db) {
    ofstream file(db.name+".db");
    if(db.tables.empty()) return;
    
    for(const auto& table : db.tables) {
        file << "TABLE " << " " << table.first << endl;
        
        // 先写入列名和类型
        const auto& columns = db.tableColumns.at(table.first);
        for(size_t i = 0; i < columns.size(); ++i) {
            file << columns[i].name << " " << columns[i].type;
            if(i < columns.size() - 1) {
                file << " ";
            }
        }
        file << endl;
        
        // 写入数据
        for(size_t i = 1; i < table.second.size(); ++i) {
            const auto& row = table.second[i];
            for(size_t j = 0; j < row.size(); ++j) {
                file << row[j];
                if(j < row.size() - 1) {
                    file << " ";
                }
            }
            file << endl;
        }
        file << "end" << endl;
    }
}

void load_database(const string& db) {
    ifstream file(db);
    if (!file.is_open()) {
        cerr << "Unable to open file: " << db << endl;
        return;
    }
    string dbName = db.substr(0, db.find_last_of('.'));
    currentDatabase = &databases[dbName];
    currentDatabase->name = dbName;
    string line, current_table;
    bool isFirstRow = true;
    
    while(getline(file, line)) {
        if(line.substr(0,5) == "TABLE") {
            current_table = line.substr(7);
            currentDatabase->tables[current_table] = {};
            isFirstRow = true;
        }
        else if(line == "end") {
            isFirstRow = true;
            continue;
        }
        else if(isFirstRow) {
            // 处理列名和类型行
            istringstream iss(line);
            vector<string> columnNames;
            vector<Column> tableColumns;
            string name, type;
            
            while(iss >> name >> type) {
                columnNames.push_back(name);
                tableColumns.push_back({name, type});
            }
            
            currentDatabase->tables[current_table].push_back(columnNames);
            currentDatabase->tableColumns[current_table] = tableColumns;
            isFirstRow = false;
        }
        else {
            // 处理数据行
            vector<string> values;
            string value;
            istringstream iss(line);
            bool inQuotes = false;
            string currentValue;
            
            for(char c : line) {
                if(c == '\'') {
                    inQuotes = !inQuotes;
                    currentValue += c;
                }
                else if(c == ' ' && !inQuotes) {
                    if(!currentValue.empty()) {
                        values.push_back(currentValue);
                        currentValue.clear();
                    }
                }
                else {
                    currentValue += c;
                }
            }
            if(!currentValue.empty()) {
                values.push_back(currentValue);
            }
            
            if(!values.empty()) {
                currentDatabase->tables[current_table].push_back(values);
            }
        }
    }
    file.close();
}
//专门的比较函数
bool compareValues(const string& value1, const string& value2, const string& type, const string& op) {
    if(type == "INTEGER") {
        int v1 = stoi(value1);
        int v2 = stoi(value2);
        if(op == "=") return v1 == v2;
        if(op == "<") return v1 < v2;
        if(op == ">") return v1 > v2;
        if(op =="!=") return v1!=v2;
    }
    else if(type == "FLOAT") {
        
        string val1 = value1;
        string val2 = value2;
        if(val1.front() == '\'') val1 = val1.substr(1, val1.length()-2);
        if(val2.front() == '\'') val2 = val2.substr(1, val2.length()-2);
        
        float v1 = stof(val1);
        float v2 = stof(val2);
        if(op == "=") return v1 == v2;
        if(op == "<") return v1 < v2;
        if(op == ">") return v1 > v2;
        if(op =="!=") return v1!=v2;
    }
    else { 
        if(op == "=") return value1 == value2;
        if(op == "<") return value1 < value2;
        if(op == ">") return value1 > value2;
        if(op =="!=") return value1!=value2;
    }
    return false;
}
//表达式计算函数
float evaluateExpression(istringstream& iss) {
    stack<float> values;
    stack<char> ops;
    
    char c;
    while(iss.get(c)) {
        if(isspace(c)) continue;
        
        if(isdigit(c) || c == '.') {
            iss.unget();
            float val;
            iss >> val;
            values.push(val);
        } else if(c == '(') {
            ops.push(c);
        } else if(c == ')') {
            while(!ops.empty() && ops.top() != '(') {
                float val2 = values.top(); values.pop();
                float val1 = values.top(); values.pop();
                char op = ops.top(); ops.pop();
                values.push(applyOp(val1, val2, op));
            }
            if(!ops.empty()) ops.pop();
        } else if(c == '+' || c == '-' || c == '*' || c == '/') {
            while(!ops.empty() && precedence(ops.top()) >= precedence(c)) {
                float val2 = values.top(); values.pop();
                float val1 = values.top(); values.pop();
                char op = ops.top(); ops.pop();
                values.push(applyOp(val1, val2, op));
            }
            ops.push(c);
        }
    }
    
    while(!ops.empty()) {
        float val2 = values.top(); values.pop();
        float val1 = values.top(); values.pop();
        char op = ops.top(); ops.pop();
        values.push(applyOp(val1, val2, op));
    }
    
    return values.top();
}

float applyOp(float a, float b, char op) {
    switch(op) {
        case '+': return a + b;
        case '-': return a - b;
        case '*': return a * b;
        case '/': return a / b;
    }
    return 0;
}

int precedence(char op) {
    if(op == '+' || op == '-') return 1;
    if(op == '*' || op == '/') return 2;
    return 0;
}
//处理WHERE条件
bool evaluateConditions(const vector<string>& row, const vector<string>& header, const vector<string>& conditions, const string& tableName) {
    bool result = true; // 初始设为 true，便于处理单一条件的情况
    bool cond1 = false, cond2 = false;
    string logicalOp;

    vector<string> condParts = conditions;

    // 处理逻辑运算符 AND 或 OR
    if (conditions.size() > 3) {
        logicalOp = conditions[3];
        condParts.erase(condParts.begin() + 3); // 移除逻辑运算符
    }

    // 处理条件1
    cond1 = evaluateSingleCondition(row, header, condParts[0], condParts[1], condParts[2], tableName);

    // 处理条件2（如果有）
    if (condParts.size() > 3) {
        cond2 = evaluateSingleCondition(row, header, condParts[3], condParts[4], condParts[5], tableName);
    }

    // 应用逻辑运算符
    if (!logicalOp.empty()) {
        if (logicalOp == "AND") {
            result = cond1 && cond2;
        } else if (logicalOp == "OR") {
            result = cond1 || cond2;
        }
    } else {
        result = cond1;
    }

    return result;
}

bool evaluateSingleCondition(const vector<string>& row, const vector<string>& header, const string& columnName, const string& op, const string& value, const string& tableName) {
    auto it = find(header.begin(), header.end(), columnName);
    if (it != header.end()) {
        size_t index = distance(header.begin(), it);
        string colType = currentDatabase->tableColumns[tableName][index].type;
        return compareValues(row[index], value, colType, op);
    } else {
        cerr << "Column " << columnName << " does not exist" << endl;
        return false;
    }
}

};
//移除两端的空白字符
string trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, last - first + 1);
}

void executeSQL(const string& filename, const string& outputFile, MiniDB& db)
{
    // ****首先清空输出文件****
    ofstream clearFile(outputFile, ios::trunc);
    clearFile.close();

    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Unable to open file: " << filename << endl;
        return;
    }

    string line;
    string sqlCommand;
    int lineNum = 0;
    
    while (getline(file, line)) {
        lineNum++;
        // sqlCommand += line + " ";
        
        // if (line.find(';')!=string::npos) {
        //     sqlCommand=trim(sqlCommand);
        //     sqlCommand.pop_back();
        size_t pos=0;
        while(pos<line.length())
        {
            size_t semicolonPos = line.find(';',pos);
            if(semicolonPos!=string::npos)//处理不规范换行的情况
            {
                string temp=line.substr(pos,semicolonPos-pos);
                sqlCommand+=temp;
            
            if(!sqlCommand.empty())
            {
                sqlCommand = trim(sqlCommand);
            try {
                // 存储原始命令用于错误报告(不想删了)
                string originalCommand = sqlCommand;
                
                istringstream iss(sqlCommand);
                string command;
                iss >> command;
                
                if (command != "CREATE" && command != "USE" && 
                    command != "INSERT" && command != "SELECT" && 
                    command != "UPDATE" && command != "DELETE" && 
                    command != "DROP") {
                    cerr << "Error at line " << lineNum << ": Invalid command" << endl;
                    cerr << "Command: " << originalCommand << endl;
                    sqlCommand.clear();
                    continue;
                }//处理不规范
                
                if(command=="CREATE")
                {
                    string type;
                    iss>>type;
                    if(type=="DATABASE")
                    {
                        string dbName;
                        iss>>dbName;
                        db.create_database(dbName);
                    }
                    else if(type=="TABLE")
                    {
                        string tableName;
                        iss>>tableName;
                        vector<string> columns;
                        string column_name,column_type,extra;
                        iss>>extra;
                        while(iss>>column_name>>column_type)
                        {
                            if(column_type.back()==',')
                            {
                                column_type.pop_back();
                            }
                            columns.push_back(column_name+" "+column_type);
                        }
                        db.create_table(tableName,columns);
                    }
                }
                else if(command=="DROP")
                {
                    string extra,name;
                    iss>>extra>>name;
                    db.drop_table(name);
                }
                else if(command=="USE")
                {
                    string extra,name;
                    iss>>extra>>name;
                    db.use_database(name);
                }
                else if (command == "SELECT") {
                    vector<string> columns;
                    string value, extra;
                    size_t innerJoinPos = sqlCommand.find("INNER JOIN");
                    
                    if (innerJoinPos != string::npos) {
                        // 处理 INNER JOIN 语句
                        string res_table, join_table, res_column, join_column, condition1, condition2;
                        string from, inner, join, on;
                        iss >> res_column >> join_column >> from >> res_table >> inner >> join >> join_table >> on >> condition1 >> extra >> condition2;
                        
                        // 去掉逗号
                        res_column.pop_back();
                        
                        vector<string> columns = {res_column, join_column};
                        vector<string> conditions = {condition1, condition2};

                        size_t where_pos = sqlCommand.find("WHERE");
                        if(where_pos != string::npos) {
                            string conditionStr = sqlCommand.substr(where_pos + 6);
                            istringstream conditionStream(conditionStr);
                            string cond;
                            while(conditionStream >> cond) {
                                if(cond.front() == '\'' && cond.back() != '\'') {
                                    string next_item;
                                    while(conditionStream >> next_item) {
                                        if(next_item.back() == '\'') {
                                            cond = cond + " " + next_item;
                                            break;
                                        }
                                        cond = cond + " " + next_item;
                                    }
                                }
                                conditions.push_back(cond);
                            }
                        }
                        
                        db.inner_join_file(res_table, join_table, columns, conditions, outputFile);
                    } else 
                    {
                        // 处理普通的 SELECT 语句
                        while (iss >> value) {
                            if (value == "FROM") {
                                break;
                            }
                            if (value == "*") {
                                // 如果是*，跳过并在后面处理
                                iss >> extra; // 读取 FROM
                                break;
                            }
                            if(value.back()==',') {
                                value.pop_back();
                            }
                            columns.push_back(value);
                        }
                        
                        string tablename;
                        iss >> tablename;
                        
                        // 如果是*，获取表的所有列名
                        if (columns.empty() || columns[0] == "*") {
                            if (db.currentDatabase && 
                                db.currentDatabase->tables.find(tablename) != db.currentDatabase->tables.end()) {
                                columns = db.currentDatabase->tables[tablename].front();
                            }
                        }
                        
                        vector<string> conditions;
                        // size_t wherePos = sqlCommand.find("WHERE");
                        // if (wherePos != string::npos) {
                        //     string conditionStr = sqlCommand.substr(wherePos + 6);
                        //     istringstream conditionStream(conditionStr);
                        //     string cond;
                        //     while (conditionStream >> cond) {
                        //         if(cond.front()=='\''||cond.back()!='\''){
                        //             string next_item;
                        //             while(conditionStream>>next_item)
                        //             {
                        //                 if(next_item.back()=='\'')
                        //                 {
                        //                     cond=cond+" "+next_item;
                        //                     break;
                        //                 }
                        //                 cond=cond+" " +next_item;
                        //             }
                        //         }
                        //         conditions.push_back(cond);
                        //     }
                        // }
                        string token;
                    while (iss >> token) {
                        if (token == "WHERE") {
                            string cond;
                            string condition;
                            getline(iss, condition);
                            condition = trim(condition);
                            // 解析条件
                            istringstream conditionStream(condition);
                            while (conditionStream >> cond) {
                                if(cond.front()=='\''&&cond.back()!='\''){
                                    string next_item;
                                    while(conditionStream>>next_item)
                                    {
                                        if(next_item.back()=='\'')
                                        {
                                            cond=cond+" "+next_item;
                                            break;
                                        }
                                        cond=cond+" " +next_item;
                                    }
                                }
                                conditions.push_back(cond);
                            }
                            break;
                        }
                    }
                    db.select_to_file(tablename, columns, conditions, outputFile);
                    }
                }
                else if(command=="INSERT")
                {
                    string into, tableName, valuesStr;
                    iss >> into >> tableName >> valuesStr;
                    size_t start = sqlCommand.find('(');  //找括号可比去括号快
                    size_t end = sqlCommand.find(')');
                    string valuesSegment = sqlCommand.substr(start + 1, end - start - 1);

                    vector<string> values;
                    string current;
                    bool inQuotes = false;
                    for (size_t i = 0; i < valuesSegment.size(); ++i) {
                        char c = valuesSegment[i];
                        if (c == '\'') {
                        inQuotes = !inQuotes;
                        current += c;
                        } else if (c == ',' && !inQuotes) {
                        // 去除首尾空格
                        current.erase(0, current.find_first_not_of(" \t"));
                        current.erase(current.find_last_not_of(" \t") + 1);
                        values.push_back(current);
                        current.clear();  //记得清空
                        } else {
                            current += c;
                        }
                    }
    // 添加最后一个值
    if (!current.empty()) {
        // 去除首尾空格
        current.erase(0, current.find_first_not_of(" \t"));
        current.erase(current.find_last_not_of(" \t") + 1);
        values.push_back(current);
    }

    db.insert_into_table(tableName, values);
}
    else if(command=="UPDATE")
{
    string tableName, set;
    iss >> tableName >> set;
    vector<pair<string,string>> updates;
    
    // 读取整个SQL命令
    size_t setPos = sqlCommand.find("SET") + 4;
    size_t wherePos = sqlCommand.find("WHERE");
    string updateSegment;
    if(wherePos != string::npos) {
        updateSegment = sqlCommand.substr(setPos, wherePos - setPos);
    } else {
        updateSegment = sqlCommand.substr(setPos);
    }

    // 解析更新表达式
    istringstream updateStream(updateSegment);
    string column, equal, expression;
    while(getline(updateStream, column, '=')) {
        // 清理column的空格
        column = trim(column);
        
        // 读取到下一个逗号或结束
        string expr;
        char c;
        bool inQuotes = false;
        while(updateStream.get(c)) {
            if(c == '\'') {
                inQuotes = !inQuotes;
                expr += c;
            }
            else if(c == ',' && !inQuotes) {
                break;
            }
            else {
                expr += c;
            }
        }
        
        // 清理表达式的空格
        expression = trim(expr);
        updates.push_back({column, expression});
    }

    // 处理WHERE条件
    vector<string> conditions;
    if(wherePos != string::npos) {
        string conditionStr = sqlCommand.substr(wherePos + 6);
        istringstream conditionStream(conditionStr);
        string cond;
        while(conditionStream >> cond) {
            if(cond.front() == '\'' && cond.back() != '\'') {
                string next_item;
                while(conditionStream >> next_item) {
                    if(next_item.back() == '\'') {
                        cond = cond + " " + next_item;
                        break;
                    }
                    cond = cond + " " + next_item;
                }
            }
            conditions.push_back(cond);
        }
    }

    db.update_table(tableName, updates, conditions);
}
                else if(command=="DELETE")
                {
                    string from, tableName, where;
                    iss >> from >> tableName;
                    vector<string> conditions;
                    string condition;
                    if (iss >> where) {
                        while (iss >> condition) {
                            if(condition.front()=='\''&&condition.back()!='\'')
                            {
                                string next_item;
                                while(iss>>next_item)
                                {
                                    if(next_item.back()=='\'')
                                    {
                                        condition=condition+" "+next_item;
                                        break;
                                    }
                                    condition=condition+" " +next_item;
                                }
                            }
                            conditions.push_back(condition);
                        }
                    }
                    db.deleteFromTable(tableName, conditions);
                }
            } catch (const exception& e) {
                cerr << "Error at line " << lineNum << ": " << e.what() << endl;
                cerr << "Command: " << sqlCommand << endl;
            }
            sqlCommand.clear();//清空sql命令
            }
            pos=semicolonPos+1;//更新位置到分号后（不用再去掉分号）
            }
            else//未找到分号
            {
                sqlCommand+=line.substr(pos)+" ";
                break;
            }
        }
    }
    file.close();//记住关闭文件（
}

int main(int argc, char* argv[])
{
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input.sql> <output.csv>" << std::endl;
        return 1;
    }

    std::string inputFile = argv[1];   //输入的sql文件
    std::string outputFile = argv[2];   //输出的csv文件

    MiniDB db;  //****每次进入函数时进行操作的db****
    executeSQL(inputFile, outputFile, db);

    return 0;
}