CREATE OR REPLACE SCRIPT historyLib() AS 
    --get all column names of a specific table
	function getAllColumns(tableName) 
	    tableName = string.upper(tableName)
		local columnNames = {}
		suc, res = pquery([[SELECT column_name FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_TABLE = :t ORDER BY COLUMN_ORDINAL_POSITION]], {t = tableName})
	  	for i=1, #res do
			table.insert(columnNames, res[i].COLUMN_NAME)
		end
		return columnNames
	end
	/*get the tablename from a SQL-Query. Assumption: tablename is first identifier
	 *make sure tablename is in uppercase and change it in the sqlCommand
	 *create HIST_tablename and swap it with tablename in a newSqlCommand
	*/
	function extractTableNameAndSqlCommand(sqlCommand)
		local tokens = sqlparsing.tokenize(sqlCommand)
		local tableName
	    for i=1,#tokens do
	    	if sqlparsing.isidentifier(tokens[i]) then
	        	tableName = tokens[i] --first identifier is tableName
	        	break
	    	end
	    end
		--make sure tableName is upper-case
		if tableName ~= nil then
			sqlCommand = sqlCommand:gsub(tableName, string.upper(tableName), 1)
			tableName = string.upper(tableName)
			local histTableName = 'HIST_'..tableName
			local newSqlCommand = sqlCommand:gsub(tableName, histTableName)
			return tableName, sqlCommand, histTableName, newSqlCommand
		end
	    return
    end
	--checks if sqlCommand contains keyword on given position
	function isKeyword(sqlCommand, keyword, position)
		local tokens = sqlparsing.tokenize(sqlCommand)
		local isMatch = false
		for i=1,#tokens do
	    	if sqlparsing.iskeyword(tokens[i]) then
	    		position = position - 1
	    		if position == 0 then
	    			if string.upper(tokens[i]) == string.upper(keyword) then
		        		isMatch = true
		        		break
	        		end
	        		--if no match -> break and end the loop
	        		break
        		end
	    	end
	    end
	    return isMatch
    end
    
--CREATE
CREATE OR REPLACE SCRIPT createHistoryTableWithView() AS 
	import('TEST.HISTORYLIB', 'history_lib')
	function createFunction(sqlCommand)
		local endTime = '9999-12-31 23:59:59.999'
		tableName, sqlCommand, histTableName = history_lib.extractTableNameAndSqlCommand(sqlCommand)
	  	query(sqlCommand)
	  	local columns = history_lib.getAllColumns(tableName)
	  	query([[RENAME TABLE ::oldName TO ::newName]], {oldName = tableName, newName = histTableName})
	    query([[ALTER TABLE ::histTable ADD ta_start TIMESTAMP WITH LOCAL TIME ZONE DEFAULT CURRENT_TIMESTAMP]], {histTable = histTableName})
		query([[ALTER TABLE ::histTable ADD ta_end TIMESTAMP WITH LOCAL TIME ZONE DEFAULT :endTime]], {histTable = histTableName, endTime = endTime})
		queryText = 'CREATE VIEW ::t AS SELECT '..table.concat(columns, ', ')..' FROM ::histTable WHERE ta_end >= CURRENT_TIMESTAMP'
		query(queryText, {t = tableName, histTable = histTableName, c = cols})
		return 'successfully created view '..tableName..' to table '..histTableName..'.'
	end

--INSERT
CREATE OR REPLACE SCRIPT insertIntoHistoryTableWithView() AS 
	import('TEST.HISTORYLIB', 'history_lib')
	function insertFunction(sqlCommand)
		tableName, sqlCommand, histTableName, newSqlCommand = history_lib.extractTableNameAndSqlCommand(sqlCommand)
		output(tableName..', '..sqlCommand..', '..histTableName..', '..newSqlCommand)
		local columnNames = history_lib.getAllColumns(tableName)
		output(table.concat(columnNames, ', '))
	  	if newSqlCommand:match("%((.-)%)") == nil then 
	  		--2.1 TODO ignore everything beyond second table
			newSqlCommand = newSqlCommand:gsub(histTableName, histTableName..' ('..table.concat(columnNames, ', ')..')', 1)
	  		newSqlCommand = newSqlCommand:gsub('*', '('..table.concat(columnNames, ', ')..')', 1)
	  	else
	  		--1.2
	  		matchingString = 'INSERT INTO '..histTableName..' VALUES '
	  		replaceString = 'INSERT INTO '..histTableName..' ('..table.concat(columnNames, ', ')..') VALUES '
	  		newSqlCommand = newSqlCommand:gsub(matchingString, replaceString)
		end
		output(newSqlCommand)
		suc, res = pquery(newSqlCommand)
		local msg = 'ERROR'
		if suc then
			msg = 'successfully inserted '..res.rows_inserted..' row(s).'
		end
		return msg
	end
	
EXECUTE SCRIPT insertIntoHistoryTableWithView() with output;
INSERT INTO HIST_MYNEWTESTTBL (ID, NAME) SELECT (ID, NAME) FROM SecondTestTable;
INSERT INTO MyNewTestTbl SELECT * FROM SecondTestTable;

--UPDATE
CREATE OR REPLACE SCRIPT updateHistoryTableWithView() AS
	import('TEST.HISTORYLIB', 'history_lib')
	function updateFunction(sqlCommand)
		local endTime = '9999-12-31 23:59:59.999'
		tableName, sqlCommand, histTableName, newSqlCommand = history_lib.extractTableNameAndSqlCommand(sqlCommand)
		local columnNames = history_lib.getAllColumns(tableName)
		suc, res = pquery('SELECT CURRENT_TIMESTAMP AS currentTimestamp') --
		local currentTime = res[1].CURRENTTIMESTAMP
		local whereStatement = newSqlCommand:match("WHERE%s+(.-)%s*$")
		local setStatement = newSqlCommand:match("SET%s+(.*)$")
		local firstUpdateQuery
		if setStatement:match("WHERE") then --WHERE clause is present in SET
			setStatement = newSqlCommand:match("SET%s+(.-)%s+WHERE")
			firstUpdateQuery = newSqlCommand:gsub('WHERE', 'WHERE ta_end = :endTime AND')
			firstUpdateQuery = firstUpdateQuery:gsub(setStatement, 'ta_end = :currentTime')
		else 
			firstUpdateQuery = newSqlCommand..' WHERE ta_end = :endTime'
			firstUpdateQuery = firstUpdateQuery:gsub(setStatement, 'ta_end = :currentTime')
		end
		suc, res = pquery(firstUpdateQuery, {currentTime = currentTime, endTime = endTime})
		local insertQuery = newSqlCommand:gsub('UPDATE', 'INSERT INTO')
		insertQuery = insertQuery:gsub('SET '..setStatement, 'SELECT '..table.concat(columnNames, ', ')..', :currentTime as ta_start FROM '..histTableName)
		insertQuery = insertQuery:gsub(histTableName, histTableName..' ('..table.concat(columnNames, ', ')..', ta_start)', 1)
		local secondUpdateQuery
		if whereStatement ~= nill then --checks if where is present
			insertQuery = insertQuery:gsub('WHERE', 'WHERE ta_end = :currentTime AND')
			secondUpdateQuery = newSqlCommand:gsub('WHERE', 'WHERE ta_end = :endTime AND')
		else 
			insertQuery = insertQuery..' WHERE ta_end = :currentTime'
			secondUpdateQuery = newSqlCommand..' WHERE ta_end = :endTime'
		end
		suc, res = pquery(insertQuery, {currentTime = currentTime})
		suc, res = pquery(secondUpdateQuery, {endTime = endTime})
		local msg
		if suc then
			msg = 'successfully updated: '..res.rows_affected..' row(s).'
		end
		return msg
	end

--DELETE
CREATE OR REPLACE SCRIPT deleteHistoryTableWithView() AS 
	import('TEST.HISTORYLIB', 'history_lib')
	function deleteFunction(sqlCommand)
		local endTime = '9999-12-31 23:59:59.999'
		tableName, sqlCommand, histTableName, newSqlCommand = history_lib.extractTableNameAndSqlCommand(sqlCommand)
		newSqlCommand = newSqlCommand:gsub('DELETE FROM '..histTableName, 'UPDATE '..histTableName..' SET ta_end = CURRENT_TIMESTAMP')
		suc, res = pquery('SELECT COUNT(*) AS CNT FROM '..tableName)
		local countBefore = res[1].CNT
		local whereStatement = newSqlCommand:match("WHERE%s+(.-)%s*$")
		if whereStatement == nil then
			newSqlCommand = newSqlCommand..' WHERE ta_end = :endTime'
		else
			newSqlCommand = newSqlCommand:gsub('WHERE', 'WHERE ta_end = :endTime AND')
		end
		suc, res = pquery(newSqlCommand, {endTime = endTime})
		local msg
		if suc then
			suc, res = pquery('SELECT COUNT(*) AS CNT FROM '..tableName)
			local countAfter = res[1].CNT
			msg = 'successfully deleted '..(countBefore-countAfter)..' row(s).'
		end
		return msg
	end

--AS OF SYSTEM TIME
CREATE OR REPLACE SCRIPT selectWithTimeTravel() AS
	import('TEST.HISTORYLIB', 'history_lib')
	function createTimeTravelQuery(sqlCommand)
		local tStamp
		local tokens = sqlparsing.tokenize(sqlCommand)
		local aostSequence = {}
	    for i=1,#tokens do
	    	if(sqlparsing.iskeyword(tokens[i]) or sqlparsing.isstringliteral(tokens[i])) then
	    		table.insert(aostSequence, tokens[i])
	    	end
	    end
	    for i=1, #aostSequence do
	    	if string.upper(aostSequence[i]) == 'AS' and string.upper(aostSequence[i+1]) == 'OF' and string.upper(aostSequence[i+2]) == 'SYSTEM' and string.upper(aostSequence[i+3]) == 'TIME' then
	    		if sqlparsing.isstringliteral(aostSequence[i+4]) then
	    			tStamp = aostSequence[i+4]	
				end
			end
	    end
		local tableName
	    for i=1,#tokens do
	    	if i >= 3 and sqlparsing.isidentifier(tokens[i]) and string.upper(tokens[i-2]) == 'FROM' then
	        	tableName = tokens[i]
	        	break
	    	end
	    end
	    sqlCommand = sqlCommand:gsub(tableName, string.upper(tableName), 1)
		tableName = string.upper(tableName)
		local histTableName = 'HIST_'..tableName
		local newSqlCommand = sqlCommand:gsub(tableName, histTableName)
		newSqlCommand = newSqlCommand:gsub(' FROM '..histTableName..' AS OF SYSTEM TIME ', ' FROM '..histTableName..' WHERE ta_start <= '..tStamp..' AND ta_end > ')
		return newSqlCommand
	end
	

	
--TRANSFORM 
CREATE OR REPLACE LUA SCRIPT mytransform() AS
	import('TEST.CREATEHISTORYTABLEWITHVIEW', 'create_script')
	import('TEST.INSERTINTOHISTORYTABLEWITHVIEW', 'insert_script')
	import('TEST.UPDATEHISTORYTABLEWITHVIEW', 'update_script')
	import('TEST.DELETEHISTORYTABLEWITHVIEW', 'delete_script')
	import('TEST.SELECTWITHTIMETRAVEL', 'timetravel_script')
	import('TEST.HISTORYLIB', 'history_lib')
	function transform_use(sqlText)
		tableName = history_lib.extractTableNameAndSqlCommand(sqlText)
		if history_lib.isKeyword(sqlText, 'SELECT', 1) and history_lib.isKeyword(sqlText, 'AS', 3) and history_lib.isKeyword(sqlText, 'OF', 4) and history_lib.isKeyword(sqlText, 'SYSTEM', 5) and history_lib.isKeyword(sqlText, 'TIME', 6) then
			sqlText = timetravel_script.createTimeTravelQuery(sqlText)
		elseif tableName ~= nil and tableName:sub(1, #'HIST_') == 'HIST_' then 
			sqlText = sqlText
		elseif history_lib.isKeyword(sqlText, 'CREATE', 1) and history_lib.isKeyword(sqlText, 'TABLE', 2) then
			msg = create_script.createFunction(sqlText)
			sqlText = 'SELECT \' '..msg..'\' ' 
		elseif history_lib.isKeyword(sqlText, 'INSERT', 1) then
			msg = insert_script.insertFunction(sqlText)
			sqlText = 'SELECT \' '..msg..'\' ' 
		elseif history_lib.isKeyword(sqlText, 'UPDATE', 1) then
			msg = update_script.updateFunction(sqlText)
			sqlText = 'SELECT \' '..msg..'\' ' 
		elseif history_lib.isKeyword(sqlText, 'DELETE', 1) then
			msg = delete_script.deleteFunction(sqlText)
			sqlText = 'SELECT \' '..msg..'\' ' 
		end
		return sqlText
	end
	
CREATE OR REPLACE LUA SCRIPT mypreprocessor() AS
	import('TEST.MYTRANSFORM', 'MYTRANSFORM')
	sqlText = sqlparsing.getsqltext() --text that user sent to DB
	sqlText = MYTRANSFORM.transform_use(sqlText)
	sqlparsing.setsqltext(sqlText)
	
