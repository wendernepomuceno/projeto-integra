create or replace package pack_upload_arquivo is
  -- Author  : WENDER
  -- Created : 06/10/2025 06:01:55
  -- Purpose :

--Ofertas UME
type r_s2200 is record(indRetif           VARCHAR2(200)
                         ,tpAmb           VARCHAR2(200)
                         ,procEmi         VARCHAR2(200)
                         ,verProc         VARCHAR2(200)
                         ,tpInsc          VARCHAR2(200)
                         ,nrInsc          VARCHAR2(200)
                         ,cpfTrab         VARCHAR2(200)
                         ,nmTrab          VARCHAR2(200)
                         ,sexo            VARCHAR2(200)
                         ,racaCor         VARCHAR2(200)
                         ,estCiv          VARCHAR2(200)
                         ,grauInstr       VARCHAR2(200)
                         ,dtNascto        VARCHAR2(200)
                         ,codMunic        VARCHAR2(200)
                         ,uf              VARCHAR2(200)
                         ,paisNascto      VARCHAR2(200)
                         ,paisNac         VARCHAR2(200)
                         ,tpLograd        VARCHAR2(200)
                         ,dscLograd       VARCHAR2(200)
                         ,nrLograd        VARCHAR2(200)
                         ,bairro          VARCHAR2(200)
                         ,cep             VARCHAR2(200)
                         ,codMunicEnd     VARCHAR2(200)
                         ,ufEnd           VARCHAR2(200)
                         ,dtAdmissao      VARCHAR2(200)
                         ,tpAdmissao      VARCHAR2(200)
                         ,indAdmissao     VARCHAR2(200)
                         ,tpRegPrev       VARCHAR2(200)
                         ,cadIni          VARCHAR2(200)
                         ,tpRegTrab       VARCHAR2(200)
                         ,nmCargo         VARCHAR2(200)
                         ,cboCargo        VARCHAR2(200)
                         ,natAtividade    VARCHAR2(200)
                         ,tpContr         VARCHAR2(200)
                         ,vrSalario       VARCHAR2(200)
                         ,orgao           VARCHAR2(200)
                         );
  type t_s2200 is table of r_s2200;
  type cc_s2200 is ref cursor return r_s2200;
/*
  type r_s2200 is record(indRetif         VARCHAR2(2)
                         ,tpAmb           VARCHAR2(2)
                         ,procEmi         VARCHAR2(2)
                         ,verProc         VARCHAR2(10)
                         ,tpInsc          VARCHAR2(2)
                         ,nrInsc          VARCHAR2(20)
                         ,cpfTrab         VARCHAR2(20)
                         ,nmTrab          VARCHAR2(200)
                         ,sexo            VARCHAR2(1)
                         ,racaCor         VARCHAR2(2)
                         ,estCiv          VARCHAR2(2)
                         ,grauInstr       VARCHAR2(2)
                         ,dtNascto        VARCHAR2(20)
                         ,codMunic        VARCHAR2(10)
                         ,uf              VARCHAR2(2)
                         ,paisNascto      VARCHAR2(5)
                         ,paisNac         VARCHAR2(5)
                         ,tpLograd        VARCHAR2(20)
                         ,dscLograd       VARCHAR2(200)
                         ,nrLograd        VARCHAR2(20)
                         ,bairro          VARCHAR2(200)
                         ,cep             VARCHAR2(20)
                         ,codMunicEnd     VARCHAR2(10)
                         ,ufEnd           VARCHAR2(2)
                         ,dtAdmissao      VARCHAR2(20)
                         ,tpAdmissao      VARCHAR2(2)
                         ,indAdmissao     VARCHAR2(2)
                         ,tpRegPrev       VARCHAR2(2)
                         ,cadIni          VARCHAR2(2)
                         ,tpRegTrab       VARCHAR2(2)
                         ,nmCargo         VARCHAR2(200)
                         ,cboCargo        VARCHAR2(20)
                         ,natAtividade    VARCHAR2(2)
                         ,tpContr         VARCHAR2(2)
                         ,vrSalario       VARCHAR2(50)
                         ,orgao           VARCHAR2(50)
                         );
  type t_s2200 is table of r_s2200;
  type cc_s2200 is ref cursor return r_s2200;
  */
  function func_arq_para_clob(p_nome_diretorio in varchar2
                             ,p_nome_arquivo   in varchar2
                             )return clob;

  procedure proc_read_file(p_nome_diretorio in varchar2
                          ,p_nome_arquivo   in varchar2
                          ,p_msg_erro       in out varchar2
                          );
                          
  function func_get_field(p_line varchar2, p_pos number) return varchar2;
    
  procedure proc_valida_S2200(p_nome_arquivo in varchar2
                             ,p_msg_erro     in out varchar2
                             );
end pack_upload_arquivo;
/
create or replace package body pack_upload_arquivo is

    /***********************************************************************************
      Autor      : Wender          Data: 06/10/2025
      Criação    :
      Solicitante: INTEGRA
    *************************************************************************************/
    function func_arq_para_clob(p_nome_diretorio in varchar2
                               ,p_nome_arquivo   in varchar2
                               )return clob is
        v_bfile      BFILE;
        v_clob       CLOB;
        v_charset    VARCHAR2(50) := 'WE8MSWIN1252'; -- Codificação comum no Windows. Mude se o CSV for UTF-8.
        v_etapa      varchar2(100);
        v_erro       varchar2(4000);
        v_valida     number;
        v_dest_offset number := 1;
        v_src_offset  number := 1;
        v_lang_context number;
    begin
        --1. Criar o BFILE, que é um ponteiro para o arquivo externo
        v_etapa := '[1] Criar o BFILE, ponteiro para o arquivo externo';
        v_bfile := BFILENAME(p_nome_diretorio, p_nome_arquivo);

        -- 2. Inicializar o CLOB
        v_etapa := '[2] Inicializar o CLOB';
        DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);

        -- 3. Abrir o arquivo BFILE
        v_etapa := '[3] Abrir o arquivo BFILE';
        DBMS_LOB.OPEN(v_bfile, DBMS_LOB.LOB_READONLY);

        -- 4. Carregar o conteúdo do BFILE diretamente para o CLOB, fazendo a conversão de binário para caractere
        v_etapa := '[4] Carregar o conteúdo - BFILE DEFAULT_LANG_CTX';
        v_lang_context := DBMS_LOB.DEFAULT_LANG_CTX;
        v_etapa := '[4] Carregar o conteúdo do BFILE diretamente para o CLOB';
        dbms_lob.loadclobfromfile(dest_lob     => v_clob
                                 ,src_bfile    => v_bfile
                                 ,amount       => DBMS_LOB.GETLENGTH(v_bfile) -- Carrega o arquivo inteiro
                                 ,dest_offset  => v_dest_offset
                                 ,src_offset   => v_src_offset
                                 ,bfile_csid   => NLS_CHARSET_ID(v_charset)  -- ID da codificação de origem
                                 ,lang_context => v_lang_context
                                 ,warning      => v_valida
                                 );

        -- 5. Fechar o BFILE
        v_etapa := '[5] Fechar o BFILE';
        DBMS_LOB.CLOSE(v_bfile);

        -- 6. Retornar o CLOB preenchido
        v_etapa := '[6] Retornar o CLOB preenchido';
        RETURN v_clob;
    exception
        when others then
            v_erro := substr('[ERRO][EXCEPTION] - func_arq_para_clob'
           ||chr(10)||'Etapa: '||v_etapa
           ||chr(10)||'Erro: '||sqlerrm,1,4000);

            -- Se o arquivo estiver aberto, feche-o antes de levantar o erro
            if DBMS_LOB.ISOPEN(v_bfile) = 1 then
                DBMS_LOB.CLOSE(v_bfile);
            end if;
            -- Limpa o CLOB temporário em caso de falha
            if DBMS_LOB.ISTEMPORARY(v_clob) = 1 then
                DBMS_LOB.FREETEMPORARY(v_clob);
            end if;

           return v_erro;
    end func_arq_para_clob;

    /***********************************************************************************
      Autor      : Wender          Data: 06/10/2025
      Criação    : Salvar o arquivo no FTP
      Solicitante: Integra
    *************************************************************************************/
    procedure proc_read_file(p_nome_diretorio in varchar2
                            ,p_nome_arquivo   in varchar2
                            ,p_msg_erro       in out varchar2
                            ) is
        
        
        
        v_primeiras_linhas  VARCHAR2(4000);
        v_etapa             VARCHAR2(100);
        v_extensao          varchar2(4);
        i                   number;
    begin
        
        v_extensao := REGEXP_SUBSTR(lower(p_nome_arquivo), '[^.]+$');
        
        if v_extensao = 'json' then
            declare
                v_arquivo  CLOB;
                v_linha1   varchar2(4000);
                v_linha2   varchar2(4000);
                v_linha3   varchar2(4000);
            begin
                -- 1. Chama a função para ler o arquivo no CLOB
                v_etapa := '[1] Chamar a função para ler o arquivo no CLOB';
                v_arquivo := func_arq_para_clob(p_nome_diretorio,p_nome_arquivo);

                -- 2. Exibe as primeiras 1000 posições do CLOB para verificar o conteúdo
                v_etapa := '[2] Exibe as primeiras 1000 posições do CLOB para verificar o conteúdo';
                v_primeiras_linhas := DBMS_LOB.SUBSTR(v_arquivo, 1000, 1);
                --v_primeiras_linhas := DBMS_LOB.SUBSTR(v_arquivo, 1000, 1001);
                
                DBMS_OUTPUT.PUT_LINE('--- CONTEÚDO DO ARQUIVO JSON (Primeiros 1000 chars) ---');
                DBMS_OUTPUT.PUT_LINE(v_primeiras_linhas);
                DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
                
                v_linha1           := DBMS_LOB.SUBSTR(v_arquivo, 3950, 1); 
                v_linha2           := DBMS_LOB.SUBSTR(v_arquivo, 3950, 3951); 
                v_linha3           := DBMS_LOB.SUBSTR(v_arquivo, 3950, 7901);

                -- insere na tabela
                INSERT INTO integra_sead_esocial(data_importacao
                                                ,nome_arquivo
                                                ,evento
                                                ,status
                                                ,tipo_arquivo
                                                ,num_linha
                                                ,txt_linha
                                                ,txt_linha2
                                                ,observacao
                                         )values(sysdate
                                                ,p_nome_arquivo
                                                ,'S-2200'
                                                ,0 --0-Novo
                                                ,v_extensao
                                                ,i
                                                ,v_linha1
                                                ,v_linha2
                                                ,v_linha3
                                                );
                -- 3. Limpa o CLOB temporário
                DBMS_LOB.FREETEMPORARY(v_arquivo);
            exception
                when others then
                    p_msg_erro := substr('[ERRO][EXCEPTION] - proc_read_file - JSON'
                              ||chr(10)||'Etapa: '||v_etapa
                              ||chr(10)||'ERRO: '||SQLERRM,1,4000);
                    DBMS_OUTPUT.PUT_LINE('Erro na execução: ' || p_msg_erro);
                    -- Garante a limpeza em caso de erro
                    if DBMS_LOB.ISTEMPORARY(v_arquivo) = 1 then
                        DBMS_LOB.FREETEMPORARY(v_arquivo);
                    end if;
            end;
            
            --Salvar
            commit;
        else
            declare
                v_arquivo   UTL_FILE.file_type;
                v_linha     varchar2(4000);
            begin
                -- abre o arquivo no diretório lógico criado no Oracle            
                v_arquivo := UTL_FILE.fopen(p_nome_diretorio, p_nome_arquivo, 'r',3950); --Tamanho máximo de buffer por linha (para evitar erro se a linha for grande).
                i := 0;
                LOOP
                    i := i+1;
                    BEGIN
                        UTL_FILE.get_line(v_arquivo, v_linha);
                        
                        if i > 1 then
                            --Cabeçalho
                            DBMS_OUTPUT.PUT_LINE('--- CONTEÚDO DO ARQUIVO CSV (Primeiros 1000 chars) ---');
                            DBMS_OUTPUT.PUT_LINE(v_primeiras_linhas);
                            DBMS_OUTPUT.PUT_LINE('----------------------------------------------------');
                            -- quebra a linha do CSV (separador ;)
                            --v_id    := TO_NUMBER(REGEXP_SUBSTR(v_linha, '[^;]+', 1, 1));
                            --v_nome  := REGEXP_SUBSTR(v_linha, '[^;]+', 1, 2);
                            --v_idade := TO_NUMBER(REGEXP_SUBSTR(v_linha, '[^;]+', 1, 3));
                            -- insere na tabela
                            INSERT INTO integra_sead_esocial(data_importacao
                                                            ,nome_arquivo
                                                            ,evento
                                                            ,status
                                                            ,tipo_arquivo
                                                            ,num_linha
                                                            ,txt_linha
                                                            ,txt_linha2
                                                            ,observacao
                                                     )values(sysdate
                                                            ,p_nome_arquivo
                                                            ,'S-2200'
                                                            ,0 --0-Novo
                                                            ,v_extensao
                                                            ,i
                                                            ,v_linha
                                                            ,null
                                                            ,null
                                                            );
                            exit;
                        end if;
                    EXCEPTION
                        WHEN NO_DATA_FOUND THEN
                            EXIT;
                    END;
                END LOOP;

                UTL_FILE.fclose(v_arquivo);
                COMMIT;
            end;
        end if;
    exception
        when others then
            p_msg_erro := substr('[ERRO][EXCEPTION] - proc_read_file'
                      ||chr(10)||'Etapa: '||v_etapa
                      ||chr(10)||'ERRO: '||SQLERRM,1,4000);
            DBMS_OUTPUT.PUT_LINE('Erro na execução: ' || p_msg_erro);
    end proc_read_file;
    
    /***********************************************************************************
      Autor      : Wender          Data: 06/10/2025
      Criação    : 
      Solicitante: Integra
    *************************************************************************************/
    -- Função auxiliar para pegar campo N
    function func_get_field(p_line varchar2, p_pos number) return varchar2 is
    begin
        return REGEXP_SUBSTR(p_line, '[^;]+', 1, p_pos);
    end;
    
    /***********************************************************************************
      Autor      : Wender          Data: 06/10/2025
      Criação    : Salvar o arquivo no FTP
      Solicitante: Integra
    *************************************************************************************/
    procedure proc_valida_S2200(p_nome_arquivo in varchar2
                               ,p_msg_erro     in out varchar2
                               ) is
      cursor crs_arq(p_nome_arquivo in varchar2) is
      select i.num_linha
            ,i.txt_linha
        from integra_sead_esocial i
       where i.nome_arquivo = p_nome_arquivo
         and i.status = 0;
      r_arq crs_arq%rowtype;
      -- 
      v_linha    number := 1; --Incluido o cabeçalho
      v_erros    varchar2(4000) := '';
      rarq       t_s2200 := t_s2200();
      --
      v_cpfTrab  varchar2(4000);
      v_etapa    varchar2(100);
    begin
        if p_nome_arquivo is null then 
            p_msg_erro := 'Arquivo não informado.';  
        else
            if crs_arq%isopen  then close crs_arq; end if;--Cursor Aberto. Fechar cursor    
        
            v_etapa :=  'Consultar Arquivo';
            --Consultar Arquivo
            open crs_arq(p_nome_arquivo);
            loop
                fetch crs_arq into r_arq;
                exit when crs_arq%notfound;
                    
                v_linha := v_linha+1;
                declare
                    v_campos DBMS_UTILITY.lname_array;
                    v_count  BINARY_INTEGER;
                    v_valor  varchar2(300);
                    i        number;
                    j        number;
                begin
                    -- dbms_output.put_line(r_arq.txt_linha);
                    -- Inserindo o primeiro
                    rarq.extend;
                    i := rarq.count;
                    j := 0;
                    loop
                        j := j+1;
                        v_valor := null;
                        
                        v_etapa :=  'Linha: '||v_linha||'. Separar por campos - Coluna: '||j;
                        v_valor := func_get_field(r_arq.txt_linha,j);
                        
                        if j = 1 then
                            v_etapa          := 'Separar campo - indRetif';
                            rarq(i).indRetif := v_valor;
                        elsif j = 2 then
                            v_etapa          := 'Separar campo - tpAmb';
                            rarq(i).tpAmb    := v_valor;
                        elsif j = 3 then
                            v_etapa          := 'Separar campo - procEmi';
                            rarq(i).procEmi  := v_valor;
                        elsif j = 4 then
                            v_etapa          := 'Separar campos - verProc';
                            rarq(i).verProc  := v_valor;
                        elsif j = 5 then
                            v_etapa          := 'Separar campos - tpInsc';
                            rarq(i).tpInsc   := v_valor;
                        elsif j = 6 then
                            v_etapa          := 'Separar campos - nrInsc';
                            rarq(i).nrInsc   := v_valor;
                        elsif j = 7 then
                            v_etapa          := 'Separar campos - cpfTrab';
                            rarq(i).cpfTrab  := v_valor;
                        elsif j = 8 then
                            v_etapa          := 'Separar campos - nmTrab';
                            rarq(i).nmTrab   := v_valor;
                        elsif j = 9 then
                            v_etapa          := 'Separar campos - sexo';
                            rarq(i).sexo     := v_valor;
                        elsif j = 10 then
                            v_etapa          := 'Separar campos - racaCor';
                            rarq(i).racaCor  := v_valor;
                        elsif j = 11 then
                            v_etapa          := 'Separar campos - estCiv';
                            rarq(i).estCiv   := v_valor;
                        elsif j = 12 then
                            v_etapa            := 'Separar campos - grauInstr';
                            rarq(i).grauInstr  := v_valor;
                        elsif j = 13 then
                            v_etapa            := 'Separar campos - dtNascto';
                            rarq(i).dtNascto   := v_valor;
                        elsif j = 14 then
                            v_etapa            := 'Separar campos - codMunic';
                            rarq(i).codMunic   := v_valor;
                        elsif j = 15 then
                            v_etapa            := 'Separar campos - uf';
                            rarq(i).uf         := v_valor;
                        elsif j = 16 then
                            v_etapa            := 'Separar campos - paisNascto';
                            rarq(i).paisNascto := v_valor;
                        elsif j = 17 then
                            v_etapa            := 'Separar campos - paisNac';
                            rarq(i).paisNac    := v_valor;
                        elsif j = 18 then
                            v_etapa            := 'Separar campos - tpLograd';
                            rarq(i).tpLograd   := v_valor;
                        elsif j = 19 then
                            v_etapa            := 'Separar campos - dscLograd';
                            rarq(i).dscLograd  := v_valor;
                        elsif j = 20 then
                            v_etapa            :=  'Separar campos - nrLograd';
                            rarq(i).nrLograd   := v_valor;
                        elsif j = 21 then
                            v_etapa            := 'Separar campos - bairro';
                            rarq(i).bairro     := v_valor;
                        elsif j = 22 then
                            v_etapa            := 'Separar campos - cep';
                            rarq(i).cep        := v_valor;
                        elsif j = 23 then
                            v_etapa             := 'Separar campos - codMunicEnd';
                            rarq(i).codMunicEnd := v_valor;
                        elsif j = 24 then
                            v_etapa             := 'Separar campos - ufEnd';
                            rarq(i).ufEnd       := v_valor;
                        elsif j = 25 then
                            v_etapa             := 'Separar campos - dtAdmissao';
                            rarq(i).dtAdmissao  := v_valor;
                        elsif j = 26 then
                            v_etapa             := 'Separar campos - tpAdmissao';
                            rarq(i).tpAdmissao  := v_valor;
                        elsif j = 27 then
                            v_etapa             := 'Separar campos - indAdmissao';
                            rarq(i).indAdmissao := v_valor;
                        elsif j = 28 then
                            v_etapa             := 'Separar campos - tpRegPrev';
                            rarq(i).tpRegPrev   := v_valor;
                        elsif j = 29 then
                            v_etapa             := 'Separar campos - cadIni';
                            rarq(i).cadIni      := v_valor;
                        elsif j = 30 then
                            v_etapa             := 'Separar campos - tpRegTrab';
                            rarq(i).tpRegTrab   := v_valor;
                        elsif j = 31 then
                            v_etapa             := 'Separar campos - nmCargo';
                            rarq(i).nmCargo     := v_valor;
                        elsif j = 32 then
                            v_etapa             := 'Separar campos - cboCargo';
                            rarq(i).cboCargo    := v_valor;
                        elsif j = 33 then
                            v_etapa              := 'Separar campos - natAtividade';
                            rarq(i).natAtividade := v_valor;
                        elsif j = 34 then
                            v_etapa             := 'Separar campos - tpContr';
                            rarq(i).tpContr     := v_valor;
                        elsif j = 35 then
                            v_etapa             := 'Separar campos - vrSalario';
                            rarq(i).vrSalario   := v_valor;
                        elsif j = 36 then
                            v_etapa             := 'Separar campos - orgao';
                            rarq(i).orgao       := v_valor;
                        else
                            exit;--Sair
                        end if;
                    end loop;
                    
                     -- CPF inválido
                    IF LENGTH(TRIM(rarq(i).cpfTrab)) != 11 OR NOT REGEXP_LIKE(rarq(i).cpfTrab, '^[0-9]+$') THEN
                        v_erros := v_erros || 'CPF inválido; ';
                    END IF;
                    
                    -- Nome vazio
                    IF rarq(i).nmTrab IS NULL OR TRIM(rarq(i).nmTrab) = '' THEN
                        v_erros := v_erros || 'Nome vazio; ';
                    END IF;

                    -- Sexo
                    IF rarq(i).sexo NOT IN ('M','F') THEN
                        v_erros := v_erros || 'Sexo inválido; ';
                    END IF;

                    -- Data nascimento
                    IF NOT REGEXP_LIKE(rarq(i).dtNascto, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') THEN
                        v_erros := v_erros || 'Data nascimento inválida; ';
                    END IF;

                    -- Data admissão
                    IF NOT REGEXP_LIKE(rarq(i).dtAdmissao, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$') THEN
                        v_erros := v_erros || 'Data admissão inválida; ';
                    END IF;

                    -- CEP
                    IF NOT REGEXP_LIKE(rarq(i).cep, '^[0-9]{8}$') THEN
                        v_erros := v_erros || 'CEP inválido; ';
                    END IF;

                    -- Salário
                    BEGIN
                        IF TO_NUMBER(rarq(i).vrSalario) <= 0 THEN
                            v_erros := v_erros || 'Salário inválido; ';
                        END IF;
                    EXCEPTION WHEN OTHERS THEN
                        v_erros := v_erros || 'Salário inválido; ';
                    END;

                    -- Cargo
                    IF rarq(i).nmCargo IS NULL OR TRIM(rarq(i).nmCargo) = '' THEN
                        v_erros := v_erros || 'Cargo vazio; ';
                    END IF;
                    
                    
                    if v_erros IS NOT NULL THEN
                        v_erros := v_linha||';'||rarq(i).cpfTrab||';'||rarq(i).nmTrab||';'||rarq(i).orgao||';'||v_erros;
                        
                        dbms_output.put_line(v_erros); 
                        v_erros := null;
                        --exit;
                    end if;
                    --
                exception
                    when others then
                         dbms_output.put_line('ERRO'
                         ||chr(10)||'Etapa: '||v_etapa
                         ||chr(10)||'Linha: '||v_linha
                         ||chr(10)||r_arq.txt_linha
                         ||chr(10)||'ERRO: '||sqlerrm
                         ); 
                         exit;
                end;
            end loop;
        end if;  
    end proc_valida_S2200;
end pack_upload_arquivo;
/
