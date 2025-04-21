/*
Stored Procedure for managing changes in DimCustomer
Last Updated: 2025-01-01
*/

CREATE OR ALTER PROCEDURE Production.usp_UpdateCustomer
	@CustomerID INT,
	@Name VARCHAR(100),
	@Age INT,
	@EmploymentStatus INT,
	@JobTitle VARCHAR(50),
	@HomeOwnershipStatus VARCHAR(50),
	@MaritalStatus VARCHAR(50),
	@PreviousLoanDefault INT
AS
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @HasChanged BIT = 0;
	
	-- Check if tracked attributes have changed
	SELECT @HasChanged = 1
	FROM Production.DimCustomer
	WHERE CustomerID = @CustomerID
	AND IsCurrent = 1
	AND (EmploymentStatus <> @EmploymentStatus
		OR HomeOwnershipStatus <> @HomeOwnershipStatus
		OR MaritalStatus <> @MaritalStatus);
	
	-- If changes detected, expire current record and insert new one
	IF @HasChanged = 1
	BEGIN
		-- Begin transaction for atomicity
		BEGIN TRANSACTION;
		
		-- Step 1: Expire the current record - DON'T update EffectiveEndDate directly
		UPDATE Production.DimCustomer
		SET IsCurrent = 0  -- Only update business columns, not temporal ones
		WHERE CustomerID = @CustomerID
		AND IsCurrent = 1;
				
		-- Step 2: Insert new current record - DON'T set temporal columns
		INSERT INTO Production.DimCustomer (
			CustomerID, Age, EmploymentStatus, 
			JobTitle, HomeOwnershipStatus, MaritalStatus,
			PreviousLoanDefault, IsCurrent
		) VALUES (
			@CustomerID, @Age, @EmploymentStatus,
			@JobTitle, @HomeOwnershipStatus, @MaritalStatus,
			@PreviousLoanDefault, 1
		);
		
		COMMIT TRANSACTION;
	END
	
	-- For new customers, just insert
	ELSE IF NOT EXISTS (SELECT 1 FROM Production.DimCustomer WHERE CustomerID = @CustomerID)
	BEGIN
		INSERT INTO Production.DimCustomer (
			CustomerID, Age, EmploymentStatus,
			JobTitle, HomeOwnershipStatus, MaritalStatus,
			PreviousLoanDefault, IsCurrent
		) VALUES (
			@CustomerID, @Age, @EmploymentStatus,
			@JobTitle, @HomeOwnershipStatus, @MaritalStatus,
			@PreviousLoanDefault, 1
		);
	END
END;
GO