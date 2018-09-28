CREATE TRIGGER [Booking].[trig_insert_Booking_tblBooking__ct] ON [Booking].[tblBooking__ct]
AFTER INSERT
AS
BEGIN /* Find corresponding CivilRegistrationIdentifier */
	UPDATE [Booking].[tblBooking__ct]
	SET [Booking].[tblBooking__ct].CprNr = i.CivilRegistrationIdentifier
	FROM [Booking].[tblBooking__ct]
	INNER JOIN inserted i ON [Booking].[tblBooking__ct].BookingId = i.BookingId /* Find corresponding Authority Code */

	UPDATE a
	SET a.MynNr = MunicipalityCode
	FROM [Booking].[tblBooking__ct] a
	INNER JOIN inserted i ON a.BookingId = i.BookingId
	INNER JOIN dbo.cprAuthorityInMem c ON a.CprNr = c.CPR_Nummer
	WHERE AuthorityType = 1 /* Find corresponding Unemployment Fund Code */

	UPDATE a
	SET a.AkaNr = MunicipalityCode
	FROM [Booking].[tblBooking__ct] a
	INNER JOIN inserted i ON a.BookingId = i.BookingId
	INNER JOIN dbo.cprAuthorityInMem c ON a.CprNr = c.CPR_Nummer
	WHERE AuthorityType = 4
END