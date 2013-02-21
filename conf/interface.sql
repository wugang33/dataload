create table cep_dataload_interface (
   id                  		 bigint                         not null,
   eventtype           		 int                            not null,
   contentTimestamp   		 datetime                       not null,
   destFilePath       		 varchar(256)                   not null,
   destFileSize      		 bigint                         null,
   state               		 int                        not null,
   stateInfo          		 varchar(8000)                  null,
   taskGenTime        		 datetime                       null,
   taskLastExecStartTime     datetime                       null,
   taskLastExecEndTime       datetime                       null,
   destTablename        	 varchar(256)                   null,
   loadSchema				 varchar(8000)					null,
   constraint PK_CEP_DATALOAD_INTERFACE primary key (id)
);

create table cep_dataload_services (
		   eventtype            int                            not null,
		   cycle                varchar(256)                   null,
		   tableschema          varchar(32767)                 null,
		   partgranularity      int                        null,
		   state                int                        null,
		   constraint PK_CEP_DATALOAD_SERVICES primary key (eventtype)
);
