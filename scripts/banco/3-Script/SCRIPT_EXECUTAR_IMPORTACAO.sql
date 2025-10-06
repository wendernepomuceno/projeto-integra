DECLARE
    v_nome_diretorio   varchar2(100);
    v_nome_arquivo     varchar2(100);
    v_txt_arquivo      clob;
    v_msg_erro         varchar2(4000);
    v_conteudo_file    clob;
    v_evento           varchar2(6);
    v_etapa            varchar2(4000);
    v_valida           number;
BEGIN
    v_nome_diretorio := 'DIR_DADOS_CSV';
    v_nome_arquivo   := 'response_1759513167178.json';
    v_evento         := 'S-2200';
    
    --Verificar se o arquivo já não foi importado
    select count(1)
      into v_valida
      from integra_sead_esocial i
     where i.nome_arquivo = v_nome_arquivo
       and i.status != 5; --5-Nunca Integrar
    
    if v_valida > 0 then
        dbms_output.put_line('O arquivo "'||v_nome_arquivo||'" já foi importado para base integra! Verifique.');
    else    
        -- 1. Chama a função para ler o arquivo no CLOB
        pack_upload_json.proc_read_file(p_nome_diretorio => v_nome_diretorio
                                       ,p_nome_arquivo   => v_nome_arquivo
                                       ,p_conteudo_file  => v_txt_arquivo
                                       ,p_msg_erro       => v_msg_erro
                                       );
        if v_msg_erro is not null then
             dbms_output.put_line('ERRO: '||v_msg_erro);
        else
            v_etapa := '[3] Inserir ';
            INSERT INTO integra_sead_esocial(data_importacao
                                            ,nome_arquivo
                                            ,evento
                                            ,txt_arquivo
                                            ,status
                                            ,observacao
                                    )values(sysdate
                                           ,v_nome_arquivo
                                           ,v_evento
                                           ,v_txt_arquivo
                                           ,0 --0-Novo
                                           ,null
                                           );
            commit;
        end if;
    end if;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Erro na execução: ' || SQLERRM);
END;
