------------------------------------------------------
--// Author: John Q Martin
--// Twitter: @SQLServerMonkey
--// Email: John.Q.Martin@jqmartin.co.uk
--// Date: 25/08/2013
-----------------------------------------------------------------------------------------
--// This script will walk through all the foreign keys in the
--// database, where they are either un-trusted or disabled it will try to check them.
--//
--// In the event of an error related to data inconsistency that means the key cannot
--// be checked the FK values that are causing the error are captured into XML format
--// along with the T-SQL statement used to identify them. This is placed into a temp
--// table and then finally outputted to the SSMS results pane.
-----------------------------------------------------------------------------------------

--// Variables to hold data for Cursor
DECLARE @ConstraintID INT;
DECLARE @FKName NVARCHAR(130);
DECLARE @FKTable NVARCHAR(261);
DECLARE @FKColumns NVARCHAR(MAX);
DECLARE @FKRefTable NVARCHAR(130);
DECLARE @FKRefColumns NVARCHAR(MAX);
DECLARE @FKStatusPre BIT;
DECLARE @FKStatusPost BIT;
DECLARE @ErrorRows TABLE(ErrorRows XML);
DECLARE @SQLCmdRef NVARCHAR(MAX) = N'ALTER TABLE {FKTable} WITH CHECK CHECK CONSTRAINT {FKName};';
DECLARE @SQLCmd NVARCHAR(MAX);
DECLARE @ErrorWhereClause NVARCHAR(MAX);
DECLARE @SQLCmdErrorRef NVARCHAR(MAX) = N'SELECT CAST((SELECT {FKColumns} FROM {FKTable} AS SrcTable WHERE NOT EXISTS (SELECT 1 FROM {Reftable} AS RefTable WHERE {WhereClause}) FOR XML PATH(''{XMLPath}'')) AS XML);';
DECLARE @SQLCmdError NVARCHAR(MAX);
DECLARE @ErrorDetail XML;

--// Table for data to drive process and status details of the execution/
CREATE TABLE #ForeignKeyStatusChecks
(
	ConstraintID INT NOT NULL,
	ForeignKeyname NVARCHAR(130) NOT NULL,
	ForeignKeyTable NVARCHAR(261) NOT NULL,
	ForeignKeyColumns NVARCHAR(MAX) NOT NULL,
	ForeignKeyReferencedTable NVARCHAR(261) NOT NULL,
	ForeignKeyReferencedColumns NVARCHAR(MAX) NOT NULL,
	ForeignKeyStatusPreCheck BIT NOT NULL,
	ForeignKeyStatusPostCheck BIT NULL,
	ErrorRows XML NULL,
    ErrorSelectStatement NVARCHAR(MAX) NULL
)
;

WITH _ForeignKeyTables
AS
(
	SELECT [SysFK].[object_id] AS [ConstraintID],
			QUOTENAME([SysFK].[name]) AS [FKName],
			QUOTENAME([SysSourceSchema].[Name])
			+ '.'
			+ QUOTENAME([SysSourceObject].[Name]) AS SorceTableName,
			QUOTENAME([SysRefSchema].[Name])
			+ '.'
			+ QUOTENAME([SysRefObject].[Name]) AS ReferenceTableName,
            CASE
                WHEN [SysFK].is_disabled = 1 OR [SysFK].is_not_trusted = 1 THEN 0
                ELSE 1
            END AS FKEnabledTrusted
	FROM [sys].[foreign_keys] AS [SysFK]
	JOIN [sys].[objects] AS [SysSourceObject] ON [SysFK].[parent_object_id] = [SysSourceObject].[object_id]
	JOIN [sys].[schemas] AS [SysSourceSchema] ON [SysSourceObject].[schema_id] = [SysSourceSchema].[schema_id]
	JOIN [sys].[objects] AS [SysRefObject] ON [SysFK].[referenced_object_id] = [SysRefObject].[object_id]
	JOIN [sys].[schemas] AS [SysRefSchema] ON [SysRefObject].[schema_id] = [SysRefSchema].[schema_id]
	WHERE [SysFK].[is_ms_shipped] = 0
		AND [SysSourceSchema].[name] != N'sys'
),

[_SourceColumnsCTE]
AS
(
--// Generate list of columns that form the foreign key in the Source table, concatenate them into single string.
SELECT DISTINCT [SysFKColumnsOuter].[constraint_object_id],
    SUBSTRING
    (
        (SELECT ',' + QUOTENAME(UPPER(LEFT([SourceColumn].[NAME],1)) + RIGHT([SourceColumn].[NAME],(LEN([SourceColumn].[NAME])-1)))
            FROM [sys].[foreign_key_columns] AS [SysFKColumns]
            JOIN [sys].[columns] AS [SourceColumn] ON [SysFKColumns].[parent_column_id] = [SourceColumn].[column_id]
                AND [SysFKColumns].[parent_object_id] = [SourceColumn].[object_id]
            WHERE [SysFKColumns].[constraint_object_id] = [SysFKColumnsOuter].[constraint_object_ID]
        FOR XML PATH('')),2,200000
    ) AS [SourceColumnList]
FROM [sys].[foreign_key_columns] AS [SysFKColumnsOuter]
),

[_ReferencedColumnsCTE]
AS
(
--// Generate list of columns that are referenced by the foreign key constraint, concatenate them into a single string.
SELECT DISTINCT [SysFKColumnsOuter].[constraint_object_id],
SUBSTRING
(
    (SELECT ',' + QUOTENAME(UPPER(LEFT([ReferencedColumn].[name],1)) + RIGHT([ReferencedColumn].[NAME],(LEN([ReferencedColumn].[NAME])-1)))
        FROM [sys].[foreign_key_columns] AS [SysFKColumns]
        JOIN [sys].[columns] AS [ReferencedColumn] ON [SysFKColumns].[referenced_column_id] = [ReferencedColumn].[column_id]
            AND [SysFKColumns].[referenced_object_id] = [ReferencedColumn].[object_id]
        WHERE [SysFKColumns].[constraint_object_id] = [SysFKColumnsOuter].[constraint_object_ID]
    FOR XML PATH('')),2,200000
) AS [ReferencedColumn]
FROM [sys].[foreign_key_columns] AS [SysFKColumnsOuter]
)
INSERT INTO #ForeignKeyStatusChecks
(ConstraintID, ForeignKeyname, ForeignKeyTable, ForeignKeyColumns, ForeignKeyReferencedTable, ForeignKeyReferencedColumns, ForeignKeyStatusPreCheck)
SELECT FKT.ConstraintID, FKT.FKName, FKT.SorceTableName, SrcCol.SourceColumnList, FKT.ReferenceTableName, RefCol.ReferencedColumn, FKT.FKEnabledTrusted
FROM _ForeignKeyTables AS FKT
JOIN _SourceColumnsCTE AS SrcCol ON FKT.ConstraintID = SrcCol.constraint_object_id
JOIN _ReferencedColumnsCTE AS RefCol ON FKT.ConstraintID = RefCol.constraint_object_id
ORDER BY FKT.FKName
;

DECLARE FKStatus CURSOR
    FORWARD_ONLY
    FOR
    
    SELECT FKCheck.ConstraintID,
	    FKCheck.ForeignKeyname,
	    FKCheck.ForeignKeyTable,
	    FKCheck.ForeignKeyColumns,
	    FKCheck.ForeignKeyReferencedTable,
	    FKCheck.ForeignKeyReferencedColumns,
	    FKCheck.ForeignKeyStatusPreCheck
    FROM #ForeignKeyStatusChecks AS FKCheck
    ;

    OPEN FKStatus;

        FETCH NEXT FROM FKStatus
        INTO
        @ConstraintID, @FKName, @FKTable, @FKColumns, @FKRefTable, @FKRefColumns, @FKStatusPre
        ;

        IF(@@FETCH_STATUS != 0)
        BEGIN
            PRINT N'<< No Data In Cursor >>';
            RETURN;
        END

        WHILE(@@FETCH_STATUS = 0)
        BEGIN

            BEGIN TRY
                
                IF(@FKStatusPre = 0)
                BEGIN

                    SET @SQLCmd = REPLACE(REPLACE(@SQLCmdRef,N'{FKTable}',@FKTable),N'{FKName}',@FKName);

                    EXEC sp_executesql @stmt = @SQLCmd;

                END

            END TRY
            BEGIN CATCH

                DECLARE @ErrorNumber INT = (SELECT ERROR_NUMBER());

                IF(@ErrorNumber = 547)
                BEGIN
                    
                    SET @ErrorWhereClause = (SELECT DISTINCT
                                                SUBSTRING
                                                (
                                                    (
                                                        SELECT ' AND '
                                                            +'[RefTable].'
                                                            + QUOTENAME(RefColumn.name)
                                                            + ' = '
                                                            + '[SrcTable].'
                                                            + QUOTENAME(ParentColumn.name)
                                                        FROM sys.foreign_keys AS SysFKInner
                                                        JOIN sys.foreign_key_columns AS SysFKCol ON SysFKInner.parent_object_id = SysFKCol.parent_object_id
                                                        JOIN sys.columns AS ParentColumn ON SysFKCol.parent_object_id = ParentColumn.[object_id]
                                                            AND SysFKCol.parent_column_id = ParentColumn.column_id
                                                        JOIN sys.columns AS RefColumn ON SysFKCol.referenced_object_id = RefColumn.[object_id]
                                                            AND SysFKCol.referenced_column_id = RefColumn.column_id
                                                        WHERE SysFKInner.[object_id] = SysFK.[object_id]
                                                        FOR XML PATH('')
                                                    ),5,200000
                                                )
                                                FROM SYS.foreign_keys AS [SysFK]
                                                WHERE [SysFK].object_id = @ConstraintID)
                    ;
                    
                    SET @SQLCmdError = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@SQLCmdErrorRef,N'{FKColumns}',@FKColumns),N'{FKTable}',@FKTable),N'{WhereClause}',@ErrorWhereClause),N'{Reftable}',@FKRefTable),N'{XMLPath}',REPLACE(REPLACE(@FKTable,N'[',''),N']',''));

                    INSERT INTO @ErrorRows
                    EXEC (@SQLCmdError)
                    ;

                    SET @ErrorDetail = (SELECT ErrorRows FROM @ErrorRows);

                    UPDATE #ForeignKeyStatusChecks
                        SET ErrorRows = @ErrorDetail,
                            ErrorSelectStatement = @SQLCmdError
                    WHERE ConstraintID = @ConstraintID
                    ;

                END
                ELSE
                BEGIN

                    DECLARE @ErrorMessage NVARCHAR(2048);
                    DECLARE @ErrorSeverity INT;
                    DECLARE @ErrorState INT;

                    SELECT @ErrorMessage = ERROR_MESSAGE(),
                        @ErrorSeverity = ERROR_SEVERITY(),
                        @ErrorState = ERROR_STATE()
                    ;

                    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
                    
                END

            END CATCH

            UPDATE #ForeignKeyStatusChecks
                    SET ForeignKeyStatusPostCheck = SourceTable.PostCheckStatus
                FROM
                (
                    SELECT
                        CASE
                            WHEN SysFK.is_disabled = 1 OR SysFK.is_not_trusted = 1 THEN 0
                            ELSE 1
                        END AS PostCheckStatus
                    FROM sys.foreign_keys AS SysFK
                    WHERE SysFK.[object_id] = @ConstraintID
                ) AS SourceTable
                WHERE ConstraintID = @ConstraintID
                ;

            FETCH NEXT FROM FKStatus
            INTO
            @ConstraintID, @FKName, @FKTable, @FKColumns, @FKRefTable, @FKRefColumns, @FKStatusPre
            ;

        END
    
    CLOSE FKStatus;
    DEALLOCATE FKStatus;

SELECT *
FROM #ForeignKeyStatusChecks
;

--// Clean Up
DROP TABLE #ForeignKeyStatusChecks;