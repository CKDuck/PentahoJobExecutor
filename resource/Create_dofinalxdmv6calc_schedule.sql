CREATE EVENT dofinalxdmv6calc_schedule ON SCHEDULE 
	EVERY 1 DAY 
	STARTS '2012-04-04 06:30:00' 
DO CALL pdistaging.dofinalxdmv6calc();