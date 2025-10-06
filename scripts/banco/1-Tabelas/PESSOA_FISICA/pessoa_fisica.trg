CREATE OR REPLACE TRIGGER VS$BIR_PESSOA_FISICA
BEFORE INSERT ON pessoa_fisica
REFERENCING OLD AS OLD NEW AS NEW
FOR EACH ROW

BEGIN
    if :NEW.ID_PESSOA_FISICA is null then
        select PESSOA_FISICA_SEQ.nextval
          into :NEW.ID_PESSOA_FISICA
          from dual;
    end if;
	 
	-- Auditoria Leve - Criacao e Modificacao
    :NEW.DATA_CRIA := SYSDATE;
    :NEW.USR_CRIA  := USER;
    :NEW.DATA_ALT  := SYSDATE;
    :NEW.USR_ALT   := USER;
END;
/

CREATE OR REPLACE TRIGGER VS$BUR_PESSOA_FISICA
BEFORE UPDATE ON pessoa_fisica
REFERENCING OLD AS OLD NEW AS NEW
FOR EACH ROW

BEGIN
    -- Auditoria Leve - Modificacao
    :NEW.DATA_ALT  := SYSDATE;
    :NEW.USR_ALT   := USER;
END;
/