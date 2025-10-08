create or replace package pack_upload_arquivo is
  -- Author  : WENDER
  -- Created : 06/10/2025 06:01:55
  -- Purpose :

  function func_arq_para_clob(p_nome_diretorio in varchar2
                             ,p_nome_arquivo   in varchar2
                             )return clob;

  procedure proc_read_file(p_nome_diretorio in varchar2
                          ,p_nome_arquivo   in varchar2
                          ,p_msg_erro       out varchar2
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
      Solicitante: SAP
    *************************************************************************************/
    procedure proc_read_file(p_nome_diretorio in varchar2
                            ,p_nome_arquivo   in varchar2
                            ,p_msg_erro       out varchar2
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
    end pack_upload_arquivo;
/
